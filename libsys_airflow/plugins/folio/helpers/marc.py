import json
import logging
import pathlib
import re
import shutil
import sqlite3
import uuid

import pandas as pd
import pymarc
import requests

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster
from folio_uuid.folio_uuid import FOLIONamespaces

from libsys_airflow.plugins.folio.audit import AuditStatus, add_audit_log
from libsys_airflow.plugins.folio.reports import srs_audit_report
from libsys_airflow.plugins.folio.remediate import _save_error

logger = logging.getLogger(__name__)


full_text_check = re.compile(
    r"(table of contents|abstract|description|sample text)", re.IGNORECASE
)

vendor_id_re = re.compile(r"\w{2,2}4")

sdr_sul_re = re.compile(r"https*:\/\/purl.stanford.edu")


def _add_electronic_holdings(field: pymarc.Field) -> bool:
    if field.indicator2 in ["0", "1"]:
        subfield_z = field.get_subfields("z")
        subfield_3 = field.get_subfields("3")
        subfield_all = " ".join(subfield_z + subfield_3)
        if full_text_check.search(subfield_all):
            return False
        return True
    return False


def _add_srs_audit_record(record: dict, connection, record_type):
    """Adds SRS Record to Audit Database"""
    cur = connection.cursor()
    if record_type == FOLIONamespaces.srs_records_holdingsrecord.value:
        hrid = record["externalIdsHolder"]["holdingsHrid"]
    else:
        hrid = record["externalIdsHolder"]["instanceHrid"]
    record_exists = cur.execute(
        "SELECT id FROM Record WHERE uuid=?", (record['id'],)
    ).fetchone()
    if record_exists:
        record_id = record_exists[0]
    else:
        cur.execute(
            """INSERT INTO Record (uuid, hrid, folio_type, current_version)
            VALUES (?,?,?,?);""",
            (record["id"], hrid, record_type, record["generation"]),
        )
        record_id = cur.lastrowid
        connection.commit()
    cur.close()
    return record_id


def _check_add_srs_records(**kwargs):
    srs_record: dict = kwargs["srs_record"]
    snapshot_id: str = kwargs["snapshot_id"]
    folio_client = kwargs["folio_client"]
    audit_connection = kwargs["audit_connection"]
    record_type = kwargs["record_type"]

    db_record_id = _add_srs_audit_record(srs_record, audit_connection, record_type)

    srs_id = srs_record["id"]
    check_record = requests.get(
        f"{folio_client.okapi_url}/source-storage/records/{srs_id}",
        headers=folio_client.okapi_headers,
    )

    match check_record.status_code:
        case 200:
            add_audit_log(db_record_id, audit_connection, AuditStatus.EXISTS.value)
            return

        case 404:
            add_audit_log(db_record_id, audit_connection, AuditStatus.MISSING.value)
            srs_record["snapshotId"] = snapshot_id
            add_result = requests.post(
                f"{folio_client.okapi_url}/source-storage/records",
                headers=folio_client.okapi_headers,
                json=srs_record,
            )
            if add_result.status_code != 201:
                _save_error(audit_connection, db_record_id, add_result)
                logger.error(
                    f"Failed to add {srs_id} http-code {add_result.status_code} error {add_result.text}"
                )

        case _:
            add_audit_log(db_record_id, audit_connection, AuditStatus.ERROR.value)
            _save_error(audit_connection, db_record_id, check_record)
            logger.error(
                f"Failed to retrieve {srs_id}, {check_record.status_code} message {check_record.text}"
            )


def _extract_e_holdings_fields(**kwargs) -> list:
    catkey = kwargs["catkey"]
    fields = kwargs["fields"]
    library = kwargs["library"]

    output = []
    for i, marc_field in enumerate(fields):
        if _add_electronic_holdings(marc_field) is False:
            continue
        row = {
            "CATKEY": catkey,
            "HOMELOCATION": "INTERNET",
            "LIBRARY": library,
            "COPY": i,
            "MAT_SPEC": None,
        }
        row["CATKEY"] = catkey
        uri = "".join(marc_field.get_subfields("u"))

        if sdr_sul_re.search(uri):
            row["HOMELOCATION"] = "SDR"

        material_type = marc_field.get_subfields("3")
        if len(material_type) > 0:
            row["MAT_SPEC"] = " ".join(material_type)
        output.append(row)
    return output


def _check_852s(fields: list) -> bool:
    """
    Checks 852 subfield 'a' and 'z' for
    """
    for field in fields:
        for a_subfield in field.get_subfields('a'):
            if "required" in a_subfield.lower():
                return True
        for z_subfield in field.get_subfields('z'):
            if z_subfield.lower().startswith("all holdings transferred to"):
                return True
    return False


def _get_library(fields596: list) -> str:
    # Default is SUL library
    library = "SUL"
    if len(fields596) < 1:
        return library
    # Use first 596 field
    subfield_a = "".join(fields596[0].get_subfields("a"))
    if "24" in subfield_a:
        library = "LAW"
    elif "25" in subfield_a or "27" in subfield_a:
        library = "HOOVER"
    elif "28" in subfield_a:
        library = "BUSINESS"
    return library


def _get_snapshot_id(folio_client):
    snapshot_id = str(uuid.uuid4())
    snapshot_result = requests.post(
        f"{folio_client.okapi_url}/source-storage/snapshots",
        headers=folio_client.okapi_headers,
        json={"jobExecutionId": snapshot_id, "status": "PARSING_IN_PROGRESS"},
    )
    if snapshot_result.status_code != 201:
        logger.error(f"Error getting snapshot {snapshot_result.text}")
    return snapshot_id


def _move_001_to_035(record: pymarc.Record) -> str:
    all001 = record.get_fields("001")

    if len(all001) < 1:
        return

    catkey = all001[0].data

    if len(all001) > 1:
        for field001 in all001[1:]:
            field035 = pymarc.Field(
                tag="035", indicators=[" ", " "], subfields=["a", field001.data]
            )
            record.add_field(field035)
            record.remove_field(field001)

    return catkey


def _srs_check_add(**kwargs):
    """
    Runs audit/remediation for a single SRS file
    """
    results_dir: pathlib.Path = kwargs["results_dir"]
    srs_type: int = kwargs["srs_type"]
    audit_connection: sqlite3.Connection = kwargs["audit_connection"]
    file_name: str = kwargs["file_name"]
    folio_client = kwargs["folio_client"]
    srs_label: str = kwargs["srs_label"]
    snapshot_id: str = kwargs["snapshot_id"]

    srs_file = results_dir / file_name

    if not srs_file.exists():
        logger.info(f"{srs_label} does not exist")
        return 0

    srs_count = 0
    logger.info(f"{srs_label} ")
    with srs_file.open() as fo:
        for line in fo.readlines():
            record = json.loads(line)
            _check_add_srs_records(
                srs_record=record,
                snapshot_id=snapshot_id,
                audit_connection=audit_connection,
                record_type=srs_type,
                folio_client=folio_client,
            )
            if not srs_count % 1_000:
                logger.info(f"Checked/Added {srs_count:,} SRS records")
            srs_count += 1

    logger.info(f"Finished audit/remediate for {srs_label} for total {srs_count:,}")
    return srs_count


def _handle_srs_iteration(**kwargs):
    iteration = kwargs["iteration"]
    folio_client = kwargs["folio_client"]
    iteration_name = iteration.split("/")[-1]
    results_dir = pathlib.Path(f"{iteration}/results/")

    audit_db_path = results_dir / "audit-remediation.db"
    audit_connection = sqlite3.connect(str(audit_db_path))

    snapshot = _get_snapshot_id(folio_client)

    logger.info(f"Starting {iteration_name} iteration")

    mhld_count = _srs_check_add(
        audit_connection=audit_connection,
        results_dir=results_dir,
        srs_type=FOLIONamespaces.srs_records_holdingsrecord.value,
        file_name="folio_srs_holdings_mhld-transformer.json",
        snapshot_id=snapshot,
        folio_client=folio_client,
        srs_label="SRS MHLDs",
    )

    bib_count = _srs_check_add(
        audit_connection=audit_connection,
        results_dir=results_dir,
        srs_type=FOLIONamespaces.srs_records_bib.value,
        file_name="folio_srs_instances_bibs-transformer.json",
        snapshot_id=snapshot,
        folio_client=folio_client,
        srs_label="SRS MARC BIBs",
    )

    srs_audit_report(audit_connection, iteration)

    audit_connection.close()
    total = mhld_count + bib_count
    logger.info(f"Total SRS Records audited/remediated {total:,}")

    return total


def discover_srs_files(*args, **kwargs):
    """
    Iterates through migration iterations directory and checks for SRS file
    existence for later auditing/remediation
    """
    airflow = kwargs.get("airflow", "/opt/airflow/")
    jobs = int(kwargs["jobs"])
    task_instance = kwargs["task_instance"]

    iterations_dir = pathlib.Path(airflow) / "migration/iterations"
    srs_iterations = []
    # Checks for SRS bibs
    for iteration in iterations_dir.iterdir():
        srs_file = iteration / "results/folio_srs_instances_bibs-transformer.json"
        if srs_file.exists():
            srs_iterations.append(str(iteration))

    shard_size = int(len(srs_iterations) / jobs)
    for i in range(jobs):
        start = i * shard_size
        end = shard_size * (i + 1)
        if i == jobs - 1:
            end = len(srs_iterations)

        task_instance.xcom_push(key=f"job-{i}", value=srs_iterations[start:end])

    logger.info(
        f"Finished SRS discovery, found {len(srs_iterations)} files and created {jobs} batches"
    )


def filter_mhlds(mhld_path: pathlib.Path):
    """
    Filters MHLD records based on strings in 852 fields
    """
    with mhld_path.open('rb') as fo:
        marc_reader = pymarc.MARCReader(fo, force_utf8=True)
        mhld_records = [record for record in marc_reader]

    start_total = len(mhld_records)

    filtered_records = []
    for record in mhld_records:
        fields852 = record.get_fields("852")
        if _check_852s(fields852) is False:
            filtered_records.append(record)

    with mhld_path.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in filtered_records:
            marc_writer.write(record)

    logger.info(
        f"Finished filtering MHLD, start {start_total:,} removed {start_total - len(filtered_records):,} records"
    )


def handle_srs_files(*args, **kwargs):
    """
    Using a list of iteration directories, calls function for checking/adding
    SRS files.
    """
    task_instance = kwargs["task_instance"]
    job = kwargs["job"]
    folio_client = kwargs["folio_client"]

    iterations = task_instance.xcom_pull(task_ids="find-srs-files", key=f"job-{job}")

    logger.info(f"Starting Check/Add SRS Bibs files for {len(iterations):,}")

    total_srs_count = 0
    for iteration in iterations:
        total_srs_count += _handle_srs_iteration(
            iteration=iteration, folio_client=folio_client
        )

    logger.info(
        f"Finished auditing/remediation of {total_srs_count:,} records for {len(iterations)}"
    )
    return total_srs_count


def marc_only(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    tsv_files = task_instance.xcom_pull(task_ids="bib-files-group", key="tsv-files")
    tsv_base = task_instance.xcom_pull(task_ids="bib-files-group", key="tsv-base")
    all_next_task_id = kwargs.get("default_task")
    marc_only_task_id = kwargs.get("marc_only_task")

    if len(tsv_files) < 1 and tsv_base is None:
        return marc_only_task_id
    return all_next_task_id


def move_marc_files(*args, **kwargs) -> str:
    """Moves MARC files to migration/data/instances"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    task_instance = kwargs["task_instance"]

    marc_filepath = task_instance.xcom_pull(task_ids="bib-files-group", key="marc-file")
    marc_path = pathlib.Path(marc_filepath)
    marc_target = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/instances/{marc_path.name}"
    )

    shutil.move(marc_path, marc_target)
    logger.info(f"{marc_path} moved to {marc_target}")

    # Moves MHLDs to Holdings
    mhld_filepath = task_instance.xcom_pull(task_ids="bib-files-group", key="mhld-file")
    if mhld_filepath is not None:
        mhld_path = pathlib.Path(mhld_filepath)
        if mhld_path.exists():
            mhld_target = pathlib.Path(
                f"{airflow}/migration/iterations/{dag.run_id}/source_data/holdings/{mhld_path.name}"
            )

            shutil.move(mhld_path, mhld_target)
            logger.info(f"{mhld_path} moved to {mhld_target}")

    return marc_path.stem


def post_marc_to_srs(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    srs_filename = kwargs.get("srs_filename")
    iteration_id = kwargs["iteration_id"]

    task_config = BatchPoster.TaskConfiguration(
        name="marc-to-srs-batch-poster",
        migration_task_type="BatchPoster",
        object_type="SRS",
        files=[{"file_name": srs_filename}],
        batch_size=kwargs.get("MAX_ENTITIES", 1000),
    )

    iteration_dir = pathlib.Path(airflow) / f"migration/iterations/{iteration_id}"

    if not (iteration_dir / f"results/{srs_filename}").exists():
        logger.info(f"{srs_filename} does not exist, existing task")
        return

    library_config = kwargs["library_config"]

    srs_batch_poster = BatchPoster(task_config, library_config, use_logging=False)

    srs_batch_poster.do_work()

    srs_batch_poster.wrap_up()

    logger.info("Finished posting MARC json to SRS")

    return srs_filename


def process(*args, **kwargs):
    marc_stem = kwargs["marc_stem"]
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    marc_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/instances/{marc_stem}.mrc"
    )
    marc_reader = pymarc.MARCReader(marc_path.read_bytes(), force_utf8=True)

    marc_records = []
    electronic_holdings = []
    for record in marc_reader:
        if record is None:
            continue
        catkey = _move_001_to_035(record)
        library = _get_library(record.get_fields("596"))
        electronic_holdings.extend(
            _extract_e_holdings_fields(
                catkey=catkey, fields=record.get_fields("856"), library=library
            )
        )
        electronic_holdings.extend(
            _extract_e_holdings_fields(
                catkey=catkey, fields=record.get_fields("956"), library=library
            )
        )
        marc_records.append(record)
        count = len(marc_records)
        if not count % 10_000:
            logger.info(f"Processed {count} MARC records")

    electronic_holdings_df = pd.DataFrame(electronic_holdings)
    electronic_holdings_df.to_csv(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/items/{marc_stem}.electronic.tsv",
        sep="\t",
        index=False,
    )

    with open(marc_path.absolute(), "wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for i, record in enumerate(marc_records):
            marc_writer.write(record)
            if not i % 10_000 and i > 0:
                logger.info(f"Writing record {i}")
