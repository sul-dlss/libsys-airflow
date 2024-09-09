import pytest
import requests_mock
from libsys_airflow.plugins.digital_bookplates.purl_fetcher import fetch_druids


@pytest.fixture
def mock_api():
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture
def mock_purl_fetcher_api_response():
    return [
        {
            'druid': 'druid:bm244yj4074',
            'object_type': 'item',
            'title': 'Beevis and Budhead Memorial Book Fund',
            'true_targets': ['SearchWorksPreview', 'ContentSearch'],
            'false_targets': ['Searchworks'],
            'collections': ['druid:nh525xs4538'],
            'updated_at': '2024-05-09T00:50:45Z',
            'published_at': '2024-05-09T00:50:45Z',
            'latest_change': '2024-05-09T00:50:45Z',
        },
        {
            'druid': 'druid:bt942vy4674',
            'object_type': 'item',
            'title': 'Stanford Law Library in Memory of Niel Peart',
            'true_targets': ['SearchWorksPreview', 'ContentSearch'],
            'false_targets': ['Searchworks'],
            'collections': ['druid:nh525xs4538'],
            'updated_at': '2024-05-09T00:50:45Z',
            'published_at': '2024-05-09T00:50:45Z',
            'latest_change': '2024-05-09T00:50:45Z',
        },
    ]


def test_purl_fetch_list(mock_api, mocker, mock_purl_fetcher_api_response):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher._purls",
        return_value=mock_purl_fetcher_api_response,
    )

    test_druids = fetch_druids.function()

    assert len(test_druids) == 2
    assert test_druids[0] == "https://purl.stanford.edu/bm244yj4074.xml"
    assert test_druids[1] == "https://purl.stanford.edu/bt942vy4674.xml"
