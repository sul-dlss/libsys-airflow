import datetime
import logging

from typing import Union
import httpx

from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from jsonpath_ng.ext import parse
from sqlalchemy.orm import Session

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from folioclient import FolioClient

FUND_NAME_EXPR = parse(
    "$.description.identifier[?(@.displayLabel == 'Symphony Fund Name')].value"
)
IMAGE_FILE_NAME_EXPR = parse("structural.contains[0].structural.contains[0].filename")
TITLE_EXPR = parse("$.label")

logger = logging.getLogger(__name__)


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


@task
def add_update_model(metadata) -> dict:
    report = {}
    if metadata["failure"] is not None:
        report["failure"] = metadata
        return report
    pg_hook = PostgresHook("digital_bookplates")

    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        bookplate = (
            session.query(DigitalBookplate)
            .where(DigitalBookplate.druid == metadata['druid'])
            .first()
        )
        if bookplate is None:
            logger.info("Bookplate is new")
            (db_id, fund_uuid) = _add_bookplate(metadata, session)
            metadata["db_id"] = db_id
            metadata["fund_uuid"] = fund_uuid
            report["new"] = metadata
        else:
            metadata = _update_bookplate(metadata, bookplate, session)
            logger.info("Bookplate was updated")
            if "reason" in metadata:
                metadata["db_id"] = bookplate.id
                report["updated"] = metadata
    return report


@task
def check_deleted_from_argo(druid_purls: list):
    argo_druids = set(
        [druid_url.split("/")[-1].split(".json")[0] for druid_url in druid_purls]
    )
    pg_hook = PostgresHook("digital_bookplates")
    deleted_info = []
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        current_bookplates = (
            session.query(DigitalBookplate.druid)
            .where(DigitalBookplate.deleted_from_argo == False)  # noqa
            .all()
        )
        current_druids = set([r[0] for r in current_bookplates])
        deleted_druids = current_druids.difference(argo_druids)
        logger.info(f"Total deleted druids {len(deleted_druids)} from Argo")
        for druid in list(deleted_druids):
            digital_bookplate = (
                session.query(DigitalBookplate)
                .where(DigitalBookplate.druid == druid)
                .first()
            )
            digital_bookplate.deleted_from_argo = True
            digital_bookplate.updated = datetime.datetime.utcnow()
            deleted_info.append(
                {
                    "druid": druid,
                    "fund_name": digital_bookplate.fund_name,
                    "title": digital_bookplate.title,
                }
            )
            session.commit()
            logger.info(f"{druid} was deleted from Argo")

    return deleted_info


@task
def fetch_druids() -> list:
    collection = _purls()
    child_druids = map(_child_druid, collection)

    druids = []
    for druid in child_druids:
        druid_id = druid.split(":")[-1]
        druids.append(f"https://purl.stanford.edu/{druid_id}.json")

    return druids


@task(max_active_tis_per_dag=10)
def extract_bookplate_metadata(druid_url: str) -> dict:

    druid = druid_url.split("/")[-1].split(".json")[0]

    metadata = {
        "druid": druid,
        "image_filename": None,
        "failure": None,
        "fund_name": None,
        "title": None,
    }
    try:
        druid_json = _get_druid_json(druid_url)
        title = TITLE_EXPR.find(druid_json)
        if len(title) > 0:
            metadata['title'] = title[0].value
        else:
            metadata["failure"] = "Missing title"
        image_file = IMAGE_FILE_NAME_EXPR.find(druid_json)
        if len(image_file) > 0:
            metadata['image_filename'] = image_file[0].value
        else:
            metadata["failure"] = "Missing image file"
        fund_name = FUND_NAME_EXPR.find(druid_json)
        if len(fund_name) > 0:
            metadata["fund_name"] = fund_name[0].value

    except httpx.HTTPStatusError as e:
        metadata["failure"] = str(e)
    return metadata


@task
def filter_updates_errors(db_results: list) -> dict:
    logger.info(db_results)
    failures, updates, new = [], [], []
    for row in db_results:
        if "failure" in row:
            failures.append(row["failure"])
        if "new" in row:
            new.append(row["new"])
        if "updates" in row:
            updates.append(row["updates"])
    logger.info(
        f"Totals: New records {len(new):,}, Failures {len(failures):,} and Updates {len(updates):,}"
    )
    return {"failures": failures, "new": new, "updates": updates}


@task
def trigger_instances_dag(**kwargs) -> bool:
    """
    'new': [
        {
            'druid': 'ef919yq2614',
            'failure': None,
            'fund_name': 'KELP',
            'title': 'The Kelp Foundation Fund',
            'image_filename': 'ef919yq2614_00_0001.jp2',
            'db_id': 4,
            'fund_uuid': 'f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4'
        },
    ]
    """
    new_funds = kwargs.get("new", [])
    if len(new_funds) < 1:
        logger.info("No new funds to trigger digital_bookplate_instances DAG")
        return True
    logger.warning(f"Should trigger {len(new_funds)} DAG runs")
    for i, fund in enumerate(new_funds):
        if fund["fund_uuid"] is not None:
            TriggerDagRunOperator(
                task_id=f"new-instance-dag-{i}",
                trigger_dag_id="digital_bookplate_instances",
                conf={"logical_date": "2023-08-28T00:00:00+00:00", "funds": [fund]},
            ).execute(
                kwargs  # type: ignore
            )
        else:
            logger.warning("Skipping new fund without fund uuid")
    return True


def _add_bookplate(metadata: dict, session: Session) -> tuple:
    new_bookplate = DigitalBookplate(
        created=datetime.datetime.utcnow(),
        updated=datetime.datetime.utcnow(),
        druid=metadata['druid'],
        title=metadata['title'],
        image_filename=metadata['image_filename'],
        fund_name=metadata['fund_name'],
        fund_uuid=_fetch_folio_fund_id(metadata['fund_name']),
    )
    session.add(new_bookplate)
    session.commit()
    return (new_bookplate.id, new_bookplate.fund_uuid)


def _child_druid(x):
    return x['druid']


def _get_druid_json(druid_url: str) -> dict:
    client = httpx.Client()
    druid_result = client.get(druid_url, timeout=30)
    druid_result.raise_for_status()
    return druid_result.json()


def _purls():
    params = "per_page=10000"
    response = httpx.get(
        "https://purl-fetcher.stanford.edu/collections/druid:nh525xs4538/purls",
        params=params,
    )

    return response.json()['purls']


def _update_bookplate(metadata, bookplate, session):
    reason = []
    for key, value in metadata.items():
        if key == "failure":
            continue
        if value != getattr(bookplate, key):
            setattr(bookplate, key, value)
            reason.append(f"{key} changed")
    if len(reason) > 0:
        metadata["reason"] = ", ".join(reason)
        fund_id = _fetch_folio_fund_id(metadata['fund_name'])
        bookplate.fund_uuid = fund_id
        metadata["fund_uuid"] = fund_id
        bookplate.updated = datetime.datetime.utcnow()
        session.commit()
    return metadata


def _fetch_folio_fund_id(fund_name) -> Union[str | None]:
    if fund_name is None:
        return None

    folio_client = _folio_client()
    folio_funds = folio_client.folio_get(
        "/finance/funds", key="funds", query_params={"query": f"""name=={fund_name}"""}
    )
    try:
        fund_id = folio_funds[0].get("id")
    except (IndexError, TypeError) as e:  # noqa
        fund_id = None

    return fund_id
