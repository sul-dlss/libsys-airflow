import httpx

from airflow.decorators import task
from jsonpath_ng.ext import parse

FUND_NAME_EXPR = parse(
    "$.description.identifier[?(@.displayLabel == 'Symphony Fund Name')].value"
)
IMAGE_FILE_NAME_EXPR = parse("structural.contains[0].structural.contains[0].filename")
TITLE_EXPR = parse("$.label")


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
        "image_file": None,
        "failure": None,
        "fund_name": None,
        "title": None,
    }
    try:
        druid_json = _get_druid_json(druid_url)
        title = TITLE_EXPR.find(druid_json)
        if len(title) > 0:
            metadata['title'] = title[0].value
        image_file = IMAGE_FILE_NAME_EXPR.find(druid_json)
        if len(image_file) > 0:
            metadata['image_file'] = image_file[0].value
        else:
            metadata["failure"] = "Missing image file"
        fund_name = FUND_NAME_EXPR.find(druid_json)
        if len(fund_name) > 0:
            metadata["fund_name"] = fund_name[0].value
    except httpx.HTTPStatusError as e:
        metadata["failure"] = str(e)
    return metadata


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
