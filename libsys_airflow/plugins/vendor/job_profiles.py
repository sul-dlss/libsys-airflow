from operator import itemgetter

from airflow.models import Variable
from libsys_airflow.plugins.folio.folio_client import FolioClient


def job_profiles(folio_client=None) -> list:
    """
    Retrieves data import job profiles from the FOLIO API.
    """
    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    job_profiles_resp = folio_client.get(
        "/data-import-profiles/jobProfiles", params={"limit": 250}
    )
    job_profiles = [
        {"id": profile["id"], "name": profile["name"]}
        for profile in job_profiles_resp["jobProfiles"]
    ]

    return sorted(job_profiles, key=itemgetter("name"))
