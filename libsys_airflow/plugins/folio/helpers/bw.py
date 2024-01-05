import datetime
import json
import logging
import pathlib

import requests

from bs4 import BeautifulSoup
from jinja2 import Template

from airflow.models import Variable
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)


def _bw_error_body(task_instance, params) -> str:
    log_url = task_instance.log_url.replace("localhost", "airflow-webserver")
    log_result = requests.get(log_url)

    email_template = Template(
        """
     <h2>Error with File {{ params.file_name }} in Task {{ ti.task_id }}</h2>

     <h3>Relationships</h3>
     <ul>
     {% for row in params.relationships[0:25] %}
     <li>Child Holdings HRID: {{ row.child_holdings_hrid}}<br>
         Parent Barcode: {{ row.parent_barcode }}
     </li>
     {% endfor %}
     </ul>
     <h3>Log:</h3>
     {{ logs }}
    """
    )

    if log_result.status_code == 200:
        log_soup = BeautifulSoup(log_result.text, 'html.parser')
        code = log_soup.find("code")
        if code:
            logs = str(code)
        else:
            logs = "<div>Could not extract log</div>"
    else:
        logs = f"<div>Error {log_result.text} trying to retrieve log</div>"
    html_body = email_template.render(params=params, ti=task_instance, logs=logs)
    return html_body


def _bw_summary_body(task_instance, file_name) -> str:
    errors = []
    for row in task_instance.xcom_pull(
        task_ids="new_bw_record", key="error", default=[]
    ):
        errors.append(row)
    total_success = 0
    for row in task_instance.xcom_pull(
        task_ids="new_bw_record", key="success", default=[]
    ):
        total_success += 1
    template = Template(
        """
    <h2>Successful Relationships for {{ file_name }}</h2>
    <p>{{ total_success }} boundwith relationships created</p>
    {% if errors|length > 0 %}
    <h2>Failed Boundwidth Relationships {{ file_name }}</h2>
    <dl>
      {% for error in errors %}
       <dt>{{ error.message }}</dt>
       <dd>
          <ul>
            <li>Child Holding ID {{ error.record.holdingsRecordId}}</li>
            <li>Parent Item ID {{ error.record.itemId }}</li>
          </ul>
        </dd>
      {% endfor %}
    </dl>
    {% endif %}
    """
    )

    return template.render(
        total_success=total_success, errors=errors, file_name=file_name
    )


def add_admin_notes(note: str, task_instance, folio_client):
    logger.info(f"Adding note {note} to holdings and items")
    count = 0
    for record in task_instance.xcom_pull(
        task_ids="new_bw_record", key="success", default=[]
    ):
        holdings_endpoint = f"/holdings-storage/holdings/{record['holdingsRecordId']}"
        holdings_record = folio_client.get(holdings_endpoint)
        holdings_record["administrativeNotes"].append(note)
        folio_client.put(holdings_endpoint, holdings_record)

        item_endpoint = f"/item-storage/items/{record['itemId']}"
        item_record = folio_client.get(item_endpoint)
        item_record["administrativeNotes"].append(note)
        folio_client.put(item_endpoint, item_record)

        if not count % 25:
            logger.info(f"Updated {count*2:,} Holdings and Items")
        count += 1
    logger.info(f"Total {count:,} Item/Holding pairs administrative notes")


def create_admin_note(sunid) -> str:
    date = datetime.datetime.utcnow().strftime("%Y%m%d")
    return f"SUL/DLSS/LibrarySystems/BWcreatedby/{sunid}/{date}"


def email_bw_summary(devs_email, task_instance):
    file_name = task_instance.xcom_pull(
        task_ids="init_bw_relationships", key="file_name"
    )
    html_content = _bw_summary_body(task_instance, file_name)
    to_addresses = [
        devs_email,
    ]
    user_email = task_instance.xcom_pull(
        task_ids="init_bw_relationships", key="user_email"
    )
    if user_email and len(user_email) > 0:
        to_addresses.append(user_email)
    send_email(
        to=to_addresses,
        subject=f"Boundwith Summary for file {file_name}",
        html_content=html_content,
    )


def email_failure(context):
    ti = context['task_instance']
    params = context['params']
    to_addresses = [Variable.get("EMAIL_DEVS")]
    email = params.get("email")
    if email:
        to_addresses.append(email)

    html_body = _bw_error_body(ti, params)

    send_email(to=to_addresses, subject=f"Error {ti.task_id}", html_content=html_body)


def discover_bw_parts_files(**kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow/")
    jobs = int(kwargs["jobs"])
    instance = kwargs["task_instance"]
    iterations = pathlib.Path(airflow) / "migration/iterations"

    bw_files = []
    for iteration in iterations.iterdir():
        bw_file = iteration / "results/boundwith_parts.json"
        if bw_file.exists():
            bw_files.append(str(bw_file))
        else:
            logger.error(f"{bw_file} doesn't exist")

    shard_size = int(len(bw_files) / jobs)

    for i in range(jobs):
        start = i * shard_size
        end = shard_size * (i + 1)
        if i == jobs - 1:
            end = len(bw_files)

        instance.xcom_push(key=f"job-{i}", value=bw_files[start:end])

    logger.info(f"Discovered {len(bw_files)} boundwidth part files for processing")


def check_add_bw(**kwargs):
    instance = kwargs["task_instance"]
    folio_client = kwargs["folio_client"]
    job_number = kwargs["job"]

    bw_file_parts = instance.xcom_pull(
        task_ids="discovery-bw-parts", key=f"job-{job_number}"
    )

    logger.info(f"Started processing {len(bw_file_parts)}")

    total_bw, total_errors = 0, 0
    for file_path in bw_file_parts:
        logger.info(f"Starting Boundwith processing for {file_path}")
        bw, errors = 0, 0
        with open(file_path) as fo:
            for line in fo.readlines():
                record = json.loads(line)
                post_result = requests.post(
                    f"{folio_client.okapi_url}/inventory-storage/bound-with-parts",
                    headers=folio_client.okapi_headers,
                    json=record,
                )
                if post_result.status_code != 201:
                    errors += 1
                    logger.error(
                        f"Failed to post {record.get('id', 'NO ID')} from {file_path} error: {post_result.text}"
                    )
                bw += 1

        logger.info(f"Processed {file_path} added {bw} errors {errors}")
        total_errors += errors
        total_bw += bw

    logger.info(f"Finished added {total_bw:,} boundwidths with {total_errors:,} errors")


def create_bw_record(**kwargs) -> dict:
    """
    Creates a boundwidth record
    """
    folio_client = kwargs["folio_client"]
    holdings_hrid = kwargs["holdings_hrid"]
    barcode = kwargs["barcode"]

    item_result = folio_client.get(
        "/inventory/items", params={"query": f"""(barcode=="{barcode}")"""}
    )

    if len(item_result['items']) < 1:
        logger.info(f"No items found for barcode {barcode}")
        return {}

    holdings_result = folio_client.get(
        "/holdings-storage/holdings", params={"query": f"""(hrid=="{holdings_hrid}")"""}
    )

    if len(holdings_result["holdingsRecords"]) < 1:
        logger.info(f"No Holdings found for HRID {holdings_hrid}")
        return {}

    item_id = item_result["items"][0]["id"]
    holdings_id = holdings_result["holdingsRecords"][0]["id"]

    return {"holdingsRecordId": holdings_id, "itemId": item_id}


def post_bw_record(**kwargs):
    folio_client = kwargs["folio_client"]
    record = kwargs["bw_parts"]
    task_instance = kwargs["task_instance"]
    try:
        post_result = folio_client.post(
            "/inventory-storage/bound-with-parts",
            payload=record,
        )
        record["id"] = post_result["id"]
        task_instance.xcom_push(key="success", value=record)
    except requests.exceptions.HTTPError as e:
        task_instance.xcom_push(
            key="error", value={"record": record, "message": str(e)}
        )
