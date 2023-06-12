import pytest  # noqa
from unittest.mock import Mock

from libsys_airflow.plugins.vendor.job_summary import job_summary


@pytest.fixture
def folio_client():
    job_summary_resp = {
        'jobExecutionId': 'd7460945-6f0c-4e74-86c9-34a8438d652e',
        'totalErrors': 3,
        'sourceRecordSummary': {
            'totalCreatedEntities': 5,
            'totalUpdatedEntities': 3,
            'totalDiscardedEntities': 1,
            'totalErrors': 1,
        },
        'instanceSummary': {
            'totalCreatedEntities': 10,
            'totalUpdatedEntities': 1,
            'totalDiscardedEntities': 2,
            'totalErrors': 2,
        },
    }
    mock_client = Mock()
    mock_client.get.return_value = job_summary_resp
    return mock_client


def test_job_summary_stats(folio_client):  # noqa: F811
    assert job_summary('d7460945-6f0c-4e74-86c9-34a8438d652e', client=folio_client) == {
        'srs_stats': {
            'totalCreatedEntities': 5,
            'totalUpdatedEntities': 3,
            'totalDiscardedEntities': 1,
            'totalErrors': 1,
        },
        'instance_stats': {
            'totalCreatedEntities': 10,
            'totalUpdatedEntities': 1,
            'totalDiscardedEntities': 2,
            'totalErrors': 2,
        },
    }
