import pytest
from unittest.mock import Mock

from libsys_airflow.plugins.vendor.job_profiles import job_profiles


@pytest.fixture
def folio_client():
    job_profiles_resp = {
        'jobProfiles': [
            {
                'id': '6409dcff-71fa-433a-bc6a-e70ad38a9604',
                'name': 'A job profile',
                'description': 'This job profile is used to create a new SRS MARC Bib record',
                'dataType': 'MARC',
                'deleted': False,
                'userInfo': {
                    'firstName': 'System',
                    'lastName': 'System',
                    'userName': 'System',
                },
                'parentProfiles': [],
                'childProfiles': [],
                'hidden': False,
                'metadata': {
                    'createdDate': '2021-01-14T14:00:00.000+00:00',
                    'createdByUserId': '00000000-0000-0000-0000-000000000000',
                    'updatedDate': '2021-01-14T15:00:00.462+00:00',
                    'updatedByUserId': '00000000-0000-0000-0000-000000000000',
                },
            },
            {
                'id': '6eefa4c6-bbf7-4845-ad82-de7fc5abd0e3',
                'name': 'Example for loading MARC',
                'description': 'Default job profile for creating MARC.',
                'dataType': 'MARC',
                'tags': {'tagList': []},
                'deleted': False,
                'userInfo': {
                    'firstName': 'System',
                    'lastName': 'System',
                    'userName': 'System',
                },
                'parentProfiles': [],
                'childProfiles': [],
                'hidden': False,
                'metadata': {
                    'createdDate': '2021-03-16T15:00:00.000+00:00',
                    'createdByUserId': '00000000-0000-0000-0000-000000000000',
                    'updatedDate': '2021-03-16T15:00:00.000+00:00',
                    'updatedByUserId': '00000000-0000-0000-0000-000000000000',
                },
            },
        ],
        'totalRecords': 2,
    }
    mock_client = Mock()
    mock_client.get.return_value = job_profiles_resp
    return mock_client


def test_job_profiles(folio_client):
    assert job_profiles(folio_client=folio_client) == [
        {'id': '6409dcff-71fa-433a-bc6a-e70ad38a9604', 'name': 'A job profile'},
        {
            'id': '6eefa4c6-bbf7-4845-ad82-de7fc5abd0e3',
            'name': 'Example for loading MARC',
        },
    ]
