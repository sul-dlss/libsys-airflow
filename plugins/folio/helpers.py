# import json
# import logging
# import pathlib
# import re
# import shutil

# import pandas as pd
# import pymarc
# import requests


# from airflow.models import Variable
# from airflow.operators.python import get_current_context


# from folio_migration_tools.migration_tasks.migration_task_base import LevelFilter


# logger = logging.getLogger(__name__)

# def _save_error_record_ids(**kwargs):
#     dag = kwargs["dag_run"]
#     records = kwargs["records"]
#     endpoint = kwargs["endpoint"]
#     error_code = kwargs["error_code"]
#     airflow = kwargs.get("airflow", pathlib.Path("/opt/airflow/"))

#     record_base = endpoint.split("/")[1]

#     error_filepath = (
#         airflow
#         / "migration/results"
#         / f"errors-{record_base}-{error_code}-{dag.run_id}.json"
#     )

#     with error_filepath.open("a") as error_file:
#         for rec in records:
#             error_file.write(json.dumps(rec))
#             error_file.write("\n")


# def archive_artifacts(*args, **kwargs):
#     """Archives JSON Instances, Items, and Holdings"""
#     dag = kwargs["dag_run"]

#     airflow = kwargs.get("airflow", "/opt/airflow")
#     tmp = kwargs.get("tmp_dir", "/tmp")

#     airflow_path = pathlib.Path(airflow)
#     tmp_path = pathlib.Path(tmp)

#     airflow_results = airflow_path / "migration/results"
#     archive_directory = airflow_path / "migration/archive"

#     if not archive_directory.exists():
#         archive_directory.mkdir()

#     for tmp_file in tmp_path.glob("*.json"):
#         try:
#             tmp_file.unlink()
#         except OSError as err:
#             logger.info(f"Cannot remove {tmp_file}: {err}")

#     for artifact in airflow_results.glob(f"*{dag.run_id}*.json"):

#         target = archive_directory / artifact.name

#         shutil.move(artifact, target)
#         logger.info("Moved {artifact} to {target}")


# def put_to_okapi(**kwargs):
#     endpoint = kwargs.get("endpoint")
#     jwt = kwargs["token"]
#     records = kwargs["records"]
#     payload_key = kwargs["payload_key"]

<<<<<<< Updated upstream
    bib_file_load = params.get("record_group")
    if bib_file_load is None:
        raise ValueError("Missing bib record load")
    logger.info(f"Retrieved MARC record {bib_file_load['marc']}")
    logger.info(f"Total number of associated tsv files {len(bib_file_load['tsv'])}")
    task_instance.xcom_push(key="marc-file", value=bib_file_load["marc"])
    task_instance.xcom_push(key="tsv-files", value=bib_file_load["tsv"])
    task_instance.xcom_push(key="tsv-base", value=bib_file_load["tsv-base"])
    task_instance.xcom_push(key="tsv-dates", value=bib_file_load["tsv-dates"])
    task_instance.xcom_push(key="mhld-file", value=bib_file_load.get("mhld"))


<<<<<<< HEAD
def move_marc_files(*args, **kwargs) -> str:
    """Moves MARC files to migration/data/instances"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    task_instance = kwargs["task_instance"]

    marc_filepath = task_instance.xcom_pull(task_ids="bib-files-group", key="marc-file")

    marc_path = pathlib.Path(marc_filepath)
    marc_target = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}/source_data/instances/{marc_path.name}")

    shutil.move(marc_path, marc_target)
    logger.info(f"{marc_path} moved to {marc_target}")

    # Moves MHLDs to Holdings
    mhld_filepath = task_instance.xcom_pull(task_ids="bib-files-group", key="mhld-file")
    if mhld_filepath is not None:
        mhld_path = pathlib.Path(mhld_filepath)
        if mhld_path.exists():
            mhld_target = pathlib.Path(f"{airflow}/migration/data/holdings/{mhld_path.name}")

            shutil.move(mhld_path, mhld_target)
            logger.info(f"{mhld_path} moved to {mhld_target}")

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
    dag = kwargs["dag_run"]

    marc_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/instances/{marc_stem}.mrc"
    )
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
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/items/{marc_stem}.electronic.tsv",
        sep="\t",
        index=False,
    )

    with open(marc_path.absolute(), "wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for i, record in enumerate(marc_records):
            marc_writer.write(record)
            if not i % 10000:
                logger.info(f"Writing record {i}")

=======
#     tenant = "sul"
#     okapi_url = Variable.get("OKAPI_URL")

#     okapi_instance_url = f"{okapi_url}{endpoint}"

#     headers = {
#         "Content-type": "application/json",
#         "user-agent": "FolioAirflow",
#         "x-okapi-token": jwt,
#         "x-okapi-tenant": tenant,
#     }

#     if payload_key:
#         payload = {payload_key: records}
#         payload["totalRecords"] = len(records)
#     else:
#         payload = records

#     update_record_result = requests.put(
#         okapi_instance_url,
#         headers=headers,
#         json=payload,
#     )

#     logger.info(f"PUT Result status code {update_record_result.status_code}")  # noqa


# def post_to_okapi(**kwargs) -> bool:
#     endpoint = kwargs.get("endpoint")
#     jwt = kwargs["token"]
>>>>>>> Stashed changes

#     records = kwargs["records"]
#     payload_key = kwargs["payload_key"]

#     tenant = "sul"
#     okapi_url = Variable.get("OKAPI_URL")

#     okapi_instance_url = f"{okapi_url}{endpoint}"

#     headers = {
#         "Content-type": "application/json",
#         "user-agent": "FolioAirflow",
#         "x-okapi-token": jwt,
#         "x-okapi-tenant": tenant,
#     }

#     payload = {payload_key: records} if payload_key else records

#     new_record_result = requests.post(
#         okapi_instance_url,
#         headers=headers,
#         json=payload,
#     )

#     logger.info(
#         f"Result status code {new_record_result.status_code} for {len(records)} records"  # noqa
#     )

#     if new_record_result.status_code > 399:
#         logger.error(new_record_result.text)
#         _save_error_record_ids(error_code=new_record_result.status_code, **kwargs)

#     if len(new_record_result.text) < 1:
#         return {}
#     return new_record_result.json()





# def setup_data_logging(transformer):
#     def transformer_data_issues(transformer, message, *args, **kwargs):
#         transformer._log(DATA_ISSUE_LVL_NUM, message, args, **kwargs)

#     # Set DATA_ISSUE logging levels
#     DATA_ISSUE_LVL_NUM = 26
#     logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")
#     logging.Logger.data_issues = transformer_data_issues

#     data_issue_file_formatter = logging.Formatter("%(message)s")
#     data_issue_file_handler = logging.FileHandler(
#         filename=str(transformer.folder_structure.data_issue_file_path),
#     )
#     data_issue_file_handler.addFilter(LevelFilter(26))
#     data_issue_file_handler.setFormatter(data_issue_file_formatter)
#     data_issue_file_handler.setLevel(26)
#     logging.getLogger().addHandler(data_issue_file_handler)


# def setup_dag_run_folders(*args, **kwargs):
#     airflow = kwargs.get("airflow", "/opt/airflow")
#     dag = kwargs["dag_run"]

#     migration_dir = pathlib.Path(f"{airflow}/migration")
#     iteration_dir = migration_dir / "iterations" / str(dag.run_id)

#     logger.info("New iteration directory {iteration_dir}")

#     iteration_dir.mkdir(parents=True)
#     source_data = iteration_dir / "source_data"

#     for folder in ["results", "reports"]:
#         folder_path = iteration_dir / folder
#         folder_path.mkdir(parents=True)

#     for record_type in ["instances", "holdings", "items"]:
#         record_path = source_data / record_type
#         record_path.mkdir(parents=True)

#     return str(iteration_dir)
