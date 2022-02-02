import logging
import pathlib
import json

import pymarc
import requests

from airflow.models import Variable

from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)

logger = logging.getLogger(__name__)

sul_config = LibraryConfiguration(
    okapi_url=Variable.get("OKAPI_URL"),
    tenant_id="sul",
    okapi_username=Variable.get("FOLIO_USER"),
    okapi_password=Variable.get("FOLIO_PASSWORD"),
    library_name="Stanford University Libraries",
    base_folder="/opt/airflow/migration",
    log_level_debug=True,
    folio_release="juniper",
    iteration_identifier="",
)


def _get_files(files: list) -> list:
    output = []
    for row in files:
        file_name = row.split("/")[-1]
        output.append({"file_name": file_name, "suppressed": False})
    return output


def preprocess_marc(*args, **kwargs):
    def move_001_to_035(record: pymarc.Record):
        all001 = record.get_fields("001")
        if len(all001) < 2:
            return
        for field001 in all001[1:]:
            field035 = pymarc.Field(
                tag="035", indicators=["", ""], subfields=["a", field001.data]
            )
            record.add_field(field035)
            record.remove_field(field001)

    for path in pathlib.Path("/opt/airflow/symphony/").glob("*.*rc"):
        marc_records = []
        marc_reader = pymarc.MARCReader(path.read_bytes())
        for record in marc_reader:
            move_001_to_035(record)
            marc_records.append(record)
        with open(path.absolute(), "wb+") as fo:
            marc_writer = pymarc.MARCWriter(fo)
            for record in marc_records:
                marc_writer.write(record)


def process_records(*args, **kwargs) -> list:
    """Function creates valid json from file of FOLIO objects"""
    pattern = kwargs.get("pattern")
    out_filename = kwargs.get("out_filename")
    records = []
    for file in pathlib.Path("/opt/airflow/migration/results").glob(pattern):
        with open(file) as fo:
            records.extend([json.loads(i) for i in fo.readlines()])

    with open(f"/tmp/{out_filename}", "w+") as fo:
        json.dump(records, fo)


def run_bibs_transformer(*args, **kwargs):
    task_instance = kwargs["task_instance"]

    files = _get_files(
        task_instance.xcom_pull(key="return_value", task_ids="move_marc_files")
    )
    bibs_configuration = BibsTransformer.TaskConfiguration(
        name="bibs-transformer",
        migration_task_type="BibsTransformer",
        hrid_handling="default",
        files=files,
        ils_flavour="voyager",  # Voyager uses 001 field, using tag001 works
    )

    bibs_transformer = BibsTransformer(
        bibs_configuration, sul_config, use_logging=False
    )

    logger.info(f"Starting bibs_tranfers work for {files}")

    bibs_transformer.do_work()

    bibs_transformer.wrap_up()


def run_holdings_tranformer(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    files = _get_files(
        task_instance.xcom_pull(key="return_value", task_ids="move_marc_files")
    )

    holdings_configuration = HoldingsMarcTransformer.TaskConfiguration(
        name="holdings-transformer",
        migration_task_type="HoldingsMarcTransformer",
        use_tenant_mapping_rules=False,
        hrid_handling="default",
        files=files,
        create_source_records=False,
        mfhd_mapping_file_name="holdingsrecord_mapping_sul.json",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        default_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    )

    holdings_transformer = HoldingsMarcTransformer(
        holdings_configuration, sul_config, use_logging=False
    )

    holdings_transformer.do_work()

    holdings_transformer.wrap_up()


def folio_login(**kwargs):
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
    jwt = kwargs["token"]

    records = kwargs["records"]
    payload_key = kwargs["payload_key"]

    tenant = "sul"
    okapi_url = Variable.get("OKAPI_URL")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {payload_key: records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(
        f"Result status code {new_record_result.status_code} for {len(records)} records" # noqa
    )

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        raise ValueError(
            f"FOLIO POST Failed with error code:{new_record_result.status_code}" # noqa
        )


def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""
    # instance_records = pathlib.Path('/tmp/instances.json').read_text()
    with open("/tmp/instances.json") as fo:
        instance_records = json.load(fo)

    _post_to_okapi(
        token=kwargs["task_instance"].xcom_pull(
            key="return_value", task_ids="folio_login"
        ),
        records=instance_records,
        endpoint="/instance-storage/batch/synchronous?upsert=true",
        payload_key="instances",
        **kwargs,
    )


def post_folio_holding_records(**kwargs):
    """Creates/overlays Holdings records in FOLIO"""
    with open("/tmp/holdings.json") as fo:
        holding_records = json.load(fo)

    _post_to_okapi(
        token=kwargs["task_instance"].xcom_pull(
            key="return_value", task_ids="folio_login"
        ),
        records=holding_records,
        endpoint="/holdings-storage/batch/synchronous?upsert=true",
        payload_key="holdingsRecords",
        **kwargs,
    )
