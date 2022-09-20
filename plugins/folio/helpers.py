import json
import logging
import pathlib
import re
import shutil

import pandas as pd
import pymarc
import requests


from airflow.models import Variable
from airflow.operators.python import get_current_context

from folioclient import FolioClient
from folio_migration_tools.migration_tasks.migration_task_base import LevelFilter


logger = logging.getLogger(__name__)


def archive_artifacts(*args, **kwargs):
    """Archives JSON Instances, Items, and Holdings"""
    dag = kwargs["dag_run"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp_dir", "/tmp")

    airflow_path = pathlib.Path(airflow)
    tmp_path = pathlib.Path(tmp)

    airflow_results = airflow_path / "migration/results"
    archive_directory = airflow_path / "migration/archive"

    if not archive_directory.exists():
        archive_directory.mkdir()

    for tmp_file in tmp_path.glob("*.json"):
        try:
            tmp_file.unlink()
        except OSError as err:
            logger.info(f"Cannot remove {tmp_file}: {err}")

    for artifact in airflow_results.glob(f"*{dag.run_id}*.json"):

        target = archive_directory / artifact.name

        shutil.move(artifact, target)
        logger.info("Moved {artifact} to {target}")


# Determines marc_only workflow
def get_bib_files(**kwargs):
    task_instance = kwargs["task_instance"]
    context = kwargs.get("context")
    if context is None:
        context = get_current_context()
    params = context.get("params")

    bib_file_load = params.get("record_group")
    if bib_file_load is None:
        raise ValueError("Missing bib record load")
    logger.info(f"Retrieved MARC record {bib_file_load['marc']}")
    logger.info(f"Total number of associated tsv files {len(bib_file_load['tsv'])}")
    task_instance.xcom_push(key="marc-file", value=bib_file_load["marc"])
    task_instance.xcom_push(key="tsv-files", value=bib_file_load["tsv"])
    task_instance.xcom_push(key="tsv-base", value=bib_file_load["tsv-base"])
    task_instance.xcom_push(key="tsv-dates", value=bib_file_load["tsv-dates"])


def move_marc_files(*args, **kwargs) -> str:
    """Moves MARC files to migration/data/instances"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    task_instance = kwargs["task_instance"]

    marc_filepath = task_instance.xcom_pull(task_ids="bib-files-group", key="marc-file")

    marc_path = pathlib.Path(marc_filepath)
    marc_target = pathlib.Path(f"{airflow}/migration/data/instances/{marc_path.name}")

    shutil.move(marc_path, marc_target)
    logger.info(f"{marc_path} moved to {marc_target}")

    return marc_path.stem


def _move_001_to_035(record: pymarc.Record) -> str:
    all001 = record.get_fields("001")

    if len(all001) < 1:
        return

    catkey = all001[0].data

    if len(all001) > 1:
        for field001 in all001[1:]:
            field035 = pymarc.Field(
                tag="035", indicators=["", ""], subfields=["a", field001.data]
            )
            record.add_field(field035)
            record.remove_field(field001)

    return catkey


full_text_check = re.compile(
    r"(table of contents|abstract|description|sample text)", re.IGNORECASE
)


def _add_electronic_holdings(field856: pymarc.Field) -> bool:
    if field856.indicator2.startswith("1"):
        subfield_z = field856.get_subfields("z")
        subfield_3 = field856.get_subfields("3")
        subfield_all = " ".join(subfield_z + subfield_3)
        if full_text_check.match(subfield_all):
            return False
    return True


def _query_for_relationships(folio_client=None) -> dict:
    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    relationships = {}
    for row in folio_client.electronic_access_relationships:
        if row["name"] == "Resource":
            relationships["0"] = row["id"]
        if row["name"] == "Version of resource":
            relationships["1"] = row["id"]
        if row["name"] == "Related resource":
            relationships["2"] = row["id"]
        if row["name"] == "No display constant generated":
            relationships["8"] = row["id"]
        if row["name"] == "No information provided":
            relationships["_"] = row["id"]
    return relationships


vendor_id_re = re.compile(r"\w{2,2}4")


def _extract_856s(catkey: str, fields: list, relationships: dict) -> list:
    properties_names = [
        "CATKEY",
        "HOMELOCATION",
        "LIBRARY",
        "LINK_TEXT",
        "MAT_SPEC",
        "PUBLIC_NOTE",
        "RELATIONSHIP",
        "URI",
        "VENDOR_CODE",
        "NOTE",
    ]
    output = []
    for field856 in fields:
        if not _add_electronic_holdings(field856):
            continue
        row = {}
        for field in properties_names:
            row[field] = None
        row["CATKEY"] = catkey
        row["URI"] = "".join(field856.get_subfields("u"))
        if row["URI"].startswith("https://purl.stanford"):
            row["HOMELOCATION"] = "SUL-SDR"
            row["LIBRARY"] = "SUL-SDR"
        else:
            row["HOMELOCATION"] = "INTERNET"
            row["LIBRARY"] = "SUL"
        material_type = field856.get_subfields("3")
        if len(material_type) > 0:
            row["MAT_SPEC"] = " ".join(material_type)
        if link_text := field856.get_subfields("y"):
            row["LINK_TEXT"] = " ".join(link_text)
        if public_note := field856.get_subfields("z"):
            row["PUBLIC_NOTE"] = " ".join(public_note)
        if all_x_subfields := field856.get_subfields("x"):
            # Checks second to last x for vendor code
            if len(all_x_subfields) >= 2 and vendor_id_re.search(all_x_subfields[-2]):
                row["VENDOR_CODE"] = all_x_subfields.pop(-2)
            # Adds remaining x subfield values to note
            row["NOTE"] = "|".join(all_x_subfields)
        if field856.indicator2 in relationships:
            row["RELATIONSHIP"] = relationships[field856.indicator2]
        else:
            row["RELATIONSHIP"] = relationships["_"]
        output.append(row)
    return output


def process_marc(*args, **kwargs):
    marc_stem = kwargs["marc_stem"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    marc_path = pathlib.Path(f"{airflow}/migration/data/instances/{marc_stem}.mrc")
    marc_reader = pymarc.MARCReader(marc_path.read_bytes())

    relationship_ids = _query_for_relationships(folio_client=kwargs.get("folio_client"))

    marc_records = []
    electronic_holdings = []
    for record in marc_reader:
        catkey = _move_001_to_035(record)
        electronic_holdings.extend(
            _extract_856s(catkey, record.get_fields("856"), relationship_ids)
        )
        marc_records.append(record)
        count = len(marc_records)
        if not count % 10_000:
            logger.info(f"Processed {count} MARC records")

    electronic_holdings_df = pd.DataFrame(electronic_holdings)
    electronic_holdings_df.to_csv(
        f"{airflow}/migration/data/items/{marc_stem}.electronic.tsv",
        sep="\t",
        index=False,
    )

    with open(marc_path.absolute(), "wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for i, record in enumerate(marc_records):
            marc_writer.write(record)
            if not i % 10000:
                logger.info(f"Writing record {i}")


def _save_error_record_ids(**kwargs):
    dag = kwargs["dag_run"]
    records = kwargs["records"]
    endpoint = kwargs["endpoint"]
    error_code = kwargs["error_code"]
    airflow = kwargs.get("airflow", pathlib.Path("/opt/airflow/"))

    record_base = endpoint.split("/")[1]

    error_filepath = (
        airflow
        / "migration/results"
        / f"errors-{record_base}-{error_code}-{dag.run_id}.json"
    )

    with error_filepath.open("a") as error_file:
        for rec in records:
            error_file.write(json.dumps(rec))
            error_file.write("\n")


def put_to_okapi(**kwargs):
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

    if payload_key:
        payload = {payload_key: records}
        payload["totalRecords"] = len(records)
    else:
        payload = records

    update_record_result = requests.put(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(f"PUT Result status code {update_record_result.status_code}")  # noqa


def post_to_okapi(**kwargs) -> bool:
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

    payload = {payload_key: records} if payload_key else records

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(
        f"Result status code {new_record_result.status_code} for {len(records)} records"  # noqa
    )

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        _save_error_record_ids(error_code=new_record_result.status_code, **kwargs)

    if len(new_record_result.text) < 1:
        return {}
    return new_record_result.json()


def process_records(*args, **kwargs) -> list:
    """Function creates valid json from file of FOLIO objects"""
    prefix = kwargs.get("prefix")
    dag = kwargs["dag_run"]

    pattern = f"{prefix}*{dag.run_id}*.json"

    out_filename = f"{kwargs.get('out_filename')}-{dag.run_id}"

    total_jobs = int(kwargs.get("jobs"))

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp", "/tmp")

    records = []
    for file in pathlib.Path(f"{airflow}/migration/results").glob(pattern):
        with open(file) as fo:
            records.extend([json.loads(i) for i in fo.readlines()])

    shard_size = int(len(records) / total_jobs)

    for i in range(total_jobs):
        start = i * shard_size
        end = shard_size * (i + 1)
        if i == total_jobs - 1:
            end = len(records)
        tmp_out_path = f"{tmp}/{out_filename}-{i}.json"
        logger.info(f"Start {start} End {end} for {tmp_out_path}")
        with open(tmp_out_path, "w+") as fo:
            json.dump(records[start:end], fo)

    return len(records)


def setup_data_logging(transformer):
    def transformer_data_issues(transformer, message, *args, **kwargs):
        transformer._log(DATA_ISSUE_LVL_NUM, message, args, **kwargs)

    # Set DATA_ISSUE logging levels
    DATA_ISSUE_LVL_NUM = 26
    logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")
    logging.Logger.data_issues = transformer_data_issues

    data_issue_file_formatter = logging.Formatter("%(message)s")
    data_issue_file_handler = logging.FileHandler(
        filename=str(transformer.folder_structure.data_issue_file_path),
    )
    data_issue_file_handler.addFilter(LevelFilter(26))
    data_issue_file_handler.setFormatter(data_issue_file_formatter)
    data_issue_file_handler.setLevel(26)
    logging.getLogger().addHandler(data_issue_file_handler)


def _apply_transforms(df, column_transforms):
    for transform in column_transforms:
        column = transform[0]
        if column in df:
            function = transform[1]
            df[column] = df[column].apply(function)
    return df


def _modify_item_type(df, libraries):
    if df["LIBRARY"].isin(libraries).any():
        df["ITEM_TYPE"] = df["ITEM_TYPE"] + " " + df["FORMAT"] + " " + df["LIBRARY"]
    return df


def _merge_notes(note_path: pathlib.Path):
    notes_df = pd.read_csv(note_path, sep="\t", dtype=object)

    match note_path.name.split(".")[-2]:
        case "circnote":
            notes_df["staffOnly"] = False
            notes_df["TYPE_NAME"] = "Note"
            column_name = "CIRCNOTE"
        case "circstaff":
            notes_df["staffOnly"] = True
            notes_df["TYPE_NAME"] = "Note"
            column_name = "CIRCNOTE"
        case "public":
            notes_df["staffOnly"] = False
            notes_df["TYPE_NAME"] = "Public"
            column_name = "PUBLIC"
        case "techstaff":
            notes_df["staffOnly"] = True
            notes_df["TYPE_NAME"] = "Tech Staff"
            column_name = "TECHSTAFF"

    notes_df = notes_df.rename(columns={column_name: "note"})
    notes_df["BARCODE"] = notes_df["BARCODE"].apply(
        lambda x: x.strip() if isinstance(x, str) else x
    )

    return notes_df


def _processes_tsv(tsv_base: str, tsv_notes: list, airflow, column_transforms, libraries):
    items_dir = pathlib.Path(f"{airflow}/migration/data/items/")

    tsv_base_df = pd.read_csv(tsv_base, sep="\t", dtype=object)
    tsv_base_df = _apply_transforms(tsv_base_df, column_transforms)
    tsv_base_df = _modify_item_type(tsv_base_df, libraries)
    new_tsv_base_path = items_dir / tsv_base.name

    tsv_base_df.to_csv(new_tsv_base_path, sep="\t", index=False)

    all_notes = []
    # Iterate on tsv notes and merge into the tsv_notes DF
    for tsv_note_path in tsv_notes:
        note_df = _merge_notes(tsv_note_path)
        all_notes.append(note_df)
        logging.info(f"Merged {len(note_df)} notes into items tsv")
        tsv_note_path.unlink()

    tsv_notes_df = pd.concat(all_notes)
    tsv_notes_name_parts = tsv_base.name.split(".")
    tsv_notes_name_parts.insert(-1, "notes")

    tsv_notes_name = ".".join(tsv_notes_name_parts)

    new_tsv_notes_path = pathlib.Path(
        f"{airflow}/migration/data_preparation/{tsv_notes_name}"
    )

    tsv_notes_df.to_csv(new_tsv_notes_path, sep="\t", index=False)

    return new_tsv_notes_path


def transform_move_tsvs(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    column_transforms = kwargs.get("column_transforms", [])
    libraries = kwargs.get("libraries", [])
    task_instance = kwargs["task_instance"]

    tsv_notes_files = task_instance.xcom_pull(
        task_ids="bib-files-group", key="tsv-files"
    )
    tsv_base_file = task_instance.xcom_pull(task_ids="bib-files-group", key="tsv-base")

    tsv_notes = [pathlib.Path(filename) for filename in tsv_notes_files]
    tsv_base = pathlib.Path(tsv_base_file)

    notes_path_name = _processes_tsv(tsv_base, tsv_notes, airflow, column_transforms, libraries)

    # Delete tsv base
    tsv_base.unlink()

    task_instance.xcom_push(key="tsv-notes", value=str(notes_path))

    return tsv_base.name
