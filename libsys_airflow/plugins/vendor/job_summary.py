from airflow.decorators import task
from airflow.models import Variable

from folioclient import FolioClient


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


@task(multiple_outputs=True)
def job_summary_task(job_execution_id: str) -> dict:
    return job_summary(job_execution_id)


def job_summary(
    job_execution_id: str,
    client=None,
) -> dict:
    folio_client = client or _folio_client()
    summary = folio_client.folio_get(
        f"/metadata-provider/jobSummary/{job_execution_id}"
    )
    return {
        "srs_stats": summary.get("sourceRecordSummary", {}),
        "instance_stats": summary.get("instanceSummary", {}),
    }
