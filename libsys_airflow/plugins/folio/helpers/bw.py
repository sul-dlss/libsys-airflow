import datetime
import logging

import httpx

from bs4 import BeautifulSoup
from jinja2 import Template

from airflow.models import Variable
from libsys_airflow.plugins.shared.utils import send_email_with_server_name

logger = logging.getLogger(__name__)


def _bw_error_body(task_instance, params) -> str:
    log_url = task_instance.log_url.replace("localhost", "airflow-webserver")
    log_result = httpx.get(log_url)

    email_template = Template(
        """
     <h2>Error with File {{ params.file_name }} in Task {{ ti.task_id }}</h2>

     <h3>Relationships</h3>
     <ul>
     {% for row in params.relationships[0:25] %}
     <li>Part Holdings HRID: {{ row.part_holdings_hrid}}<br>
         Principle Barcode: {{ row.principle_barcode }}
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
            <li>Parts Holding ID {{ error.record.holdingsRecordId}}</li>
            <li>Principle Item ID {{ error.record.itemId }}</li>
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
        holdings_record = folio_client.folio_get(holdings_endpoint)
        holdings_record["administrativeNotes"].append(note)
        folio_client.folio_put(holdings_endpoint, holdings_record)

        item_endpoint = f"/item-storage/items/{record['itemId']}"
        item_record = folio_client.folio_get(item_endpoint)
        item_record["administrativeNotes"].append(note)
        folio_client.folio_put(item_endpoint, item_record)

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
    send_email_with_server_name(
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

    send_email_with_server_name(
        to=to_addresses, subject=f"Error {ti.task_id}", html_content=html_body
    )


def create_bw_record(**kwargs) -> dict:
    """
    Creates a boundwidth record
    """
    folio_client = kwargs["folio_client"]
    holdings_hrid = kwargs["holdings_hrid"]
    barcode = kwargs["barcode"]

    item_result = folio_client.folio_get(
        "/inventory/items",
        key="items",
        query_params={"query": f"""(barcode=="{barcode}")"""},
    )

    if len(item_result) < 1:
        logger.info(f"No items found for barcode {barcode}")
        return {}

    holdings_result = folio_client.folio_get(
        "/holdings-storage/holdings",
        key="holdingsRecords",
        query_params={"query": f"""(hrid=="{holdings_hrid}")"""},
    )

    if len(holdings_result) < 1:
        logger.info(f"No Holdings found for HRID {holdings_hrid}")
        return {}

    item_id = item_result[0]["id"]
    holdings_id = holdings_result[0]["id"]

    return {"holdingsRecordId": holdings_id, "itemId": item_id}


def post_bw_record(**kwargs):
    folio_client = kwargs["folio_client"]
    record = kwargs["bw_parts"]
    task_instance = kwargs["task_instance"]
    try:
        post_result = folio_client.folio_post(
            "/inventory-storage/bound-with-parts",
            payload=record,
        )
        record["id"] = post_result["id"]
        task_instance.xcom_push(key="success", value=record)
    except httpx.HTTPError as e:
        task_instance.xcom_push(
            key="error", value={"record": record, "message": str(e)}
        )
