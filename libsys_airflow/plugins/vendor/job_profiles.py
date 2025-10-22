from operator import itemgetter

from airflow.sdk import Variable
from folioclient import FolioClient


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

    job_profiles_resp = folio_client.folio_get(
        "/data-import-profiles/jobProfiles",
        key="jobProfiles",
        query_params={"limit": 250},
    )
    job_profiles = [
        {"id": profile["id"], "name": profile["name"]} for profile in job_profiles_resp
    ]

    return sorted(job_profiles, key=itemgetter("name"))


def get_job_profile_name(job_profile_id):
    _job_profiles = job_profiles()
    job_profile = next(
        job_profile
        for job_profile in _job_profiles
        if job_profile['id'] == job_profile_id
    )
    return job_profile['name']
