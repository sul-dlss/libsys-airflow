import datetime
import json
import logging
import pathlib

import requests

from jinja2 import Template

from airflow.utils.email import send_email

logger = logging.getLogger(__name__)


def _bw_summary_body(task_instance) -> str:
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
    <h2>Successful Relationships</h2>
    <p>{{ total_success }} boundwith relationships created</p>
    {% if errors|length > 0 %}
    <h2>Failed Boundwidth Relationships</h2>
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

    return template.render(total_success=total_success, errors=errors)


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


def email_bw_summary(user_email, devs_email, task_instance):
    html_content = _bw_summary_body(task_instance)
    to_addresses = [
        devs_email,
    ]
    if user_email and len(user_email) > 0:
        to_addresses.append(user_email)
    send_email(to=to_addresses, subject="Boundwith Summary", html_content=html_content)


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
