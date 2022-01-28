import logging
import requests
import pathlib
import json
import sys
from airflow.models import Variable

from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from migration_tools.migration_tasks.holdings_marc_transformer import HoldingsMarcTransformer

logger = logging.getLogger(__name__)

sul_config = LibraryConfiguration(
    okapi_url=Variable.get("OKAPI_URL"),
    tenant_id="sul",
    okapi_username=Variable.get("FOLIO_USER"),
    okapi_password=Variable.get("FOLIO_PASSWORD"),
    library_name="Stanford University Libraries",
    base_folder="/opt/airflow/migration",
    log_level_debug=True,
    folio_release="iris",
    iteration_identifier="",
)

def _get_files(files: list) -> list:
    output = []
    for row in files:
        file_name = row.split("/")[-1]
        output.append({ "file_name": file_name, "suppressed": False})
    return output

def run_bibs_transformer(*args, **kwargs):
    task_instance = kwargs["task_instance"]

    files = _get_files(task_instance.xcom_pull(key="return_value", task_ids="move_marc_files"))
    bibs_configuration= BibsTransformer.TaskConfiguration(
        name="bibs-transformer",
        migration_task_type="BibsTransformer",
        hrid_handling="default",
        files=files,
        ils_flavour="voyager" # Voyager uses 001 field, using until 001 is available 
    )

    bibs_transformer = BibsTransformer(bibs_configuration, sul_config)

    bibs_transformer.do_work()

def run_holdings_tranformer(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    files = _get_files(task_instance.xcom_pull(key="return_value", task_ids="move_marc_files"))

    holdings_configuration = HoldingsMarcTransformer.TaskConfiguration(
        name="holdings-transformer",
        migration_task_type="HoldingsMarcTransformer",
        use_tenant_mapping_rules=False,
        hrid_handling="default",
        files=files,
        mfhd_mapping_file_name="holdingsrecord_mapping_sul.json",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        default_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed"
    )

    holdings_transformer = HoldingsMarcTransformer(holdings_configuration, sul_config)

    holdings_transformer.do_work()

def FolioLogin(**kwargs):
    """Logs into FOLIO and returns Okapi token."""
    okapi_url = Variable.get("OKAPI_URL")
    username = Variable.get("FOLIO_USER")
    password = Variable.get("FOLIO_PASSWORD")
    tenant = "sul"

    data = {"username": username, "password": password}
    headers = {"Content-type": "application/json", "x-okapi-tenant": tenant}

    url = f"{okapi_url}/authn/login"
    result = requests.post(url, json=data, headers=headers)

    if result.status_code == 201:  # Valid token created and returned
        return result.headers.get("x-okapi-token")

    result.raise_for_status()


def _post_to_okapi(**kwargs):
    endpoint = kwargs.get("endpoint")
    jwt = FolioLogin(**kwargs)

    records = kwargs["records"]
    tenant = "sul"
    okapi_url = Variable.get("OKAPI_URL")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {"instances": records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(new_record_result.status_code)

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        raise ValueError(
            f"FOLIO POST Failed with error code:{new_record_result.status_code}"
        )


def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""
    # instance_records = pathlib.Path('/tmp/instances.json').read_text()
    with open("/tmp/instances.json") as fo:
        instance_records = json.load(fo)

    # _post_to_okapi(
    #     records=instance_records,
    #     endpoint="/instance-storage/batch/synchronous?upsert=true",
    #     **kwargs,
    # )
