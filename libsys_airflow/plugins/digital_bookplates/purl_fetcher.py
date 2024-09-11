import httpx

from airflow.decorators import task


@task
def fetch_druids() -> list:
    collection = _purls()
    child_druids = map(_child_druid, collection)

    druids = []
    for druid in child_druids:
        druid_id = druid.split(":")[-1]
        druids.append(f"https://purl.stanford.edu/{druid_id}.json")

    return druids


def _child_druid(x):
    return x['druid']


def _purls():
    params = "per_page=10000"
    response = httpx.get(
        "https://purl-fetcher.stanford.edu/collections/druid:nh525xs4538/purls",
        params=params,
    )

    return response.json()['purls']
