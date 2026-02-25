import logging

from typing import Union

from airflow.sdk import task, Variable
from airflow.providers.standard.exceptions import AirflowException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from libsys_airflow.plugins.shared import utils

from folioclient import FolioClient
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


def _get_bookplate_metadata_with_fund_uuids() -> dict:
    funds = {}
    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        for f in (
            session.query(DigitalBookplate)
            .where(DigitalBookplate.fund_uuid.is_not(None))
            .all()
        ):
            funds[f.fund_uuid] = {
                "fund_name": f.fund_name,
                "druid": f.druid,
                "image_filename": f.image_filename,
                "title": f.title,
            }

    return funds


def launch_digital_bookplate_979_dag(**kwargs) -> str:
    """
    Triggers digital_bookplate_979 DAG with kwargs
    """
    instance_uuid: str = kwargs["instance_uuid"]
    funds: list = kwargs["funds"]
    dag_run_id: Union[str, None] = kwargs.get("run_id")
    dag_payload = {instance_uuid: funds}
    execution_date = utils.execution_date()

    if dag_run_id is None:
        dag_run_id = f"manual__{execution_date}"

    TriggerDagRunOperator(
        task_id="launch_digital_bookplate_979_dag",
        trigger_dag_id="digital_bookplate_979",
        trigger_run_id=dag_run_id,
        logical_date=execution_date,
        conf={"druids_for_instance_id": dag_payload},
    )

    logger.info(f"Triggers 979 DAG with dag_id {dag_run_id}")
    return dag_run_id


def launch_poll_for_979_dags_email(**kwargs):
    """
    Triggers poll_for_digital_bookplate_979s_email DAG with kwargs
    """
    dag_runs: list = kwargs["dag_runs"]
    email: Union[str, None] = kwargs.get("email")

    execution_date = utils.execution_date()
    run_id = f"manual__{execution_date}"

    TriggerDagRunOperator(
        task_id="launch_poll_for_979_dags_email",
        trigger_dag_id="poll_for_digital_bookplate_979s_email",
        trigger_run_id=run_id,
        logical_date=execution_date,
        conf={"dag_runs": dag_runs, "email": email},
    )
    logger.info(f"Triggers polling DAG for 979 DAG runs with dag_id {run_id}")


def _new_bookplates(funds: list) -> dict:
    """
    Transforms new funds list into dictionary with fund_uuid as key
    """
    bookplates = {}
    for row in funds:
        bookplates[row["fund_uuid"]] = {
            "fund_name": row["fund_name"],
            "druid": row["druid"],
            "image_filename": row["image_filename"],
            "title": row["title"],
        }

    return bookplates


def _dedup_bookplates(bookplates: list) -> list:
    return [
        dict(bp_tuple)
        for bp_tuple in {tuple(sorted(bookplate.items())) for bookplate in bookplates}
    ]


def _package_instances(poline_id: str) -> dict:
    "Returns a dictionary of instance_id: poline_id"
    folio_client = _folio_client()
    package_titles = folio_client.folio_get(
        "/orders/titles",
        key="titles",
        query_params={"query": f"""(poLineId=="{poline_id}")"""},
    )
    if len(package_titles) < 1:
        logger.info(
            f"No titles linked to package but PO Line {poline_id} is marked as package."
        )
        return {}
    else:
        # per JSON schema instanceId is not a required field for orders/titles
        return {
            title.get("instanceId"): poline_id
            for title in package_titles
            if title.get("instanceId") is not None
        }


def _add_poline_bookplate(
    poline_id: str, bookplates_polines: dict, bookplate: dict
) -> list:
    bookplates_list = bookplates_polines[poline_id]["bookplate_metadata"]
    bookplates_list.append(bookplate)
    return _dedup_bookplates(bookplates_list)


@task(max_active_tis_per_dag=5)
def bookplate_funds_polines(**kwargs) -> dict:
    """
    Checks if fund Id from invoice lines contains bookplate fund
    If no bookplate fund was used, this task returns an empty dict {}
    This task gets digital bookplates data from the table or uses
    a list of new funds from params. Groups by po line Id
    """
    bookplates_polines: dict = {}
    invoice_lines = kwargs["invoice_lines"]
    params = kwargs.get("params", {})
    funds = params.get("funds", [])
    if len(funds) > 0:
        logger.info("Getting bookplates data from list of new funds")
        bookplates = _new_bookplates(funds)
    else:
        logger.info("Getting bookplates data from the table")
        bookplates = _get_bookplate_metadata_with_fund_uuids()

    for row in invoice_lines:
        fund_distribution = row.get("fundDistributions")
        poline_id = row.get("poLineId")
        if fund_distribution and poline_id:
            for fund in fund_distribution:
                bookplate = bookplates.get(fund["fundId"])
                if bookplate is None:
                    next
                else:
                    if poline_id in bookplates_polines:
                        new_bookplates_data = _add_poline_bookplate(
                            poline_id, bookplates_polines, bookplate
                        )
                        bookplates_polines[poline_id][
                            "bookplate_metadata"
                        ] = new_bookplates_data
                    else:
                        bookplates_polines[poline_id] = {
                            "bookplate_metadata": [bookplate]
                        }

    if len(bookplates_polines) == 0:
        logger.info("No bookplate funds were used")

    return bookplates_polines  # -> instances_from_po_lines


@task(max_active_tis_per_dag=5)
def instances_from_po_lines(**kwargs) -> dict:
    """
    Given a dict of po lines with list of bookplate metadata, retrieves the instanceId
    It is possible for an instance to have multiple 979's
    {
      "5513c3d7-7c6b-45ea-a875-09798b368873": {
        "bookplate_metadata": [
          {"fund_name": "...", "druid": "...", "image_filename": "...", "title": "..."},
          {"fund_name": "...", "druid": "...", "image_filename": "...", "title": "..."},
        ]
      },
      ...
    }
    """
    folio_client = _folio_client()
    instances_bookplates: dict = {}
    instances_poline: dict = {}
    po_lines_funds = kwargs["po_lines_funds"]
    for poline_id in po_lines_funds.keys():
        order_line = folio_client.folio_get(f"/orders-storage/po-lines/{poline_id}")
        is_package = order_line.get("isPackage")
        if is_package is True:
            logger.info(f"PO Line {poline_id} is for a package.")
            instances_poline.update(_package_instances(poline_id))
        else:
            instance_id = order_line.get("instanceId")
            if instance_id is None:
                logger.info(
                    f"PO Line {poline_id} not linked to a FOLIO Instance record"
                )
                continue
            else:
                instances_poline.update({instance_id: poline_id})

    for instance_id, poline_id in instances_poline.items():
        bookplate_metadata = po_lines_funds[poline_id]["bookplate_metadata"]
        if instance_id in instances_bookplates:
            instances_bookplates[instance_id].extend(bookplate_metadata)
        else:
            instances_bookplates[instance_id] = bookplate_metadata

    for k, v in instances_bookplates.items():
        instances_bookplates[k] = _dedup_bookplates(v)

    return instances_bookplates


@task
def add_979_marc_tags(druid_instances: dict) -> dict:
    """
    "242c6000-8485-5fcd-9b5e-adb60788ca59": [
        { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
    ]
    Contruct a 979 tag with the
    fund name in subfield f, druid in subfield b, image filename in subfield c, and title in subfield d:
    """

    marc_instance_tags: dict = {'979': []}
    for _instance_uuid, druids in druid_instances.items():
        for tag_data in druids:
            fund_name = tag_data.get('fund_name', None)
            if fund_name is None:
                fund_name = tag_data.get('druid', '')
            marc_instance_tags['979'].append(
                {
                    'ind1': ' ',
                    'ind2': ' ',
                    'subfields': [
                        {'f': fund_name},
                        {'b': f"druid:{tag_data.get('druid', '')}"},
                        {'c': tag_data.get('image_filename', '')},
                        {'d': tag_data.get('title', '')},
                    ],
                }
            )

    return marc_instance_tags  # -> add_marc_tags_to_record


@task
def instance_id_for_druids(**kwargs) -> list:
    druids_instances = kwargs["druid_instances"]
    if druids_instances is None:
        return []
    return list(druids_instances.keys())[0]


@task
def add_marc_tags_to_record(**kwargs):
    # marc_tag:
    """
    {'979':
        [
            {'ind1': ' ', 'ind2': ' ', 'subfields': [
                    {'f': 'ABBOTT'}, {'b': 'druid:ws066yy0421'},
                    {'c': 'ws066yy0421_00_0001.jp2'},
                    {'d': 'The The Donald P. Abbott Fund for Marine Invertebrates'}
                ]
            },
        ]
    }
    """
    marc_tags = kwargs["marc_instance_tags"]
    instance_id = kwargs["instance_uuid"]
    folio_add_marc_tags = utils.FolioAddMarcTags()
    if folio_add_marc_tags.put_folio_records(marc_tags, instance_id):
        return True
    else:
        raise AirflowException("Failed to add marc tags to record.")


@task
def retrieve_druids_for_instance_task(**kwargs):
    """
    Retrieves and returns a dictionary from the DAG params
    """
    params = kwargs.get("params", {})
    return params.get("druids_for_instance_id", {})


@task
def trigger_digital_bookplate_979_task(**kwargs):
    instances = kwargs["instances"]
    dag_run_ids = []
    total_instances: int = 0
    for row in instances:
        if len(row) < 1:
            continue

        total_instances += len(row)
        for instance_uuid, funds in row.items():
            run_id = launch_digital_bookplate_979_dag(
                instance_uuid=instance_uuid, funds=funds
            )
            dag_run_ids.append(run_id)
    logger.info(f"Total incoming instances {total_instances}")
    return dag_run_ids


@task
def trigger_poll_for_979s_task(**kwargs):
    dag_run_ids = kwargs["dag_runs"]
    launch_poll_for_979_dags_email(dag_runs=dag_run_ids)
