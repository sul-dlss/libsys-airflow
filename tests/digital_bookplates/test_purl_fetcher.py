import httpx
import pytest
import requests_mock


from libsys_airflow.plugins.digital_bookplates.purl_fetcher import (
    extract_bookplate_metadata,
    fetch_druids,
)


sample_druids = {
    "dk886cn6583": {  #
        "druid": "druid:dk886cn6583",
        "label": "The Fund for the Future of Libraries",
        "description": {
            "identifier": [{"displayLabel": "Symphony Fund Name", "value": "FUNDLIB"}]
        },
        "structural": {
            "contains": [
                {"structural": {"contains": [{"filename": "future_library.jp2"}]}}
            ]
        },
    },
    "vz805mb3388": {  # No image file
        "druid": "druid:vz805mb3388",
        "label": "In Memory of Samuel Clements, Gift of Tom Swift",
        "description": {
            "identifier": [{"displayLabel": "Symphony Fund Name", "value": "TWAIN"}]
        },
    },
}


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


def mock_httpx_client():
    def mock_response(request):
        response = None
        if request.url.path.endswith("vz805mb3388.json"):
            response = httpx.Response(
                status_code=200, json=sample_druids["vz805mb3388"]
            )
        if request.url.path.endswith("dk886cn6583.json"):
            response = httpx.Response(
                status_code=200, json=sample_druids["dk886cn6583"]
            )
        if response is None:
            response = httpx.Response(status_code=404)
        return response

    return httpx.Client(transport=httpx.MockTransport(mock_response))


def test_purl_fetch_list(mock_api, mocker, mock_purl_fetcher_api_response):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher._purls",
        return_value=mock_purl_fetcher_api_response,
    )

    test_druids = fetch_druids.function()

    assert len(test_druids) == 2
    assert test_druids[0] == "https://purl.stanford.edu/bm244yj4074.json"
    assert test_druids[1] == "https://purl.stanford.edu/bt942vy4674.json"


def test_expected_druid(mocker):

    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher.httpx.Client",
        return_value=mock_httpx_client(),
    )

    druid_url = "https://purl.stanford.edu/ddk886cn6583.json"

    metadata = extract_bookplate_metadata.function(druid_url)

    assert metadata["title"].startswith("The Fund for the Future of Libraries")
    assert metadata["fund_name"].startswith("FUNDLIB")
    assert metadata["failure"] is None
    assert metadata["image_file"].startswith("future_library.jp2")


def test_missing_image_file(mocker):

    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher.httpx.Client",
        return_value=mock_httpx_client(),
    )

    druid_url = "https://purl.stanford.edu/vz805mb3388.json"

    metadata = extract_bookplate_metadata.function(druid_url)

    assert metadata["druid"] == "vz805mb3388"
    assert metadata["title"].startswith(
        "In Memory of Samuel Clements, Gift of Tom Swift"
    )
    assert metadata["fund_name"].startswith("TWAIN")
    assert metadata["failure"].startswith("Missing image file")


def test_missing_purl(mocker):

    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher.httpx.Client",
        return_value=mock_httpx_client(),
    )

    druid_url = "https://purl.stanford.edu/pn034fs6161.json"

    metadata = extract_bookplate_metadata.function(druid_url)

    assert metadata['druid'] == "pn034fs6161"

    assert metadata['failure'].startswith("Client error '404 Not Found'")
