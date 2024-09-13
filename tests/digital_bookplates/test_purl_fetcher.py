import datetime

import httpx
import pytest
import requests_mock

from airflow.providers.postgres.hooks.postgres import PostgresHook
from pytest_mock_resources import create_sqlite_fixture, Rows

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from libsys_airflow.plugins.digital_bookplates.purl_fetcher import (
    add_update_model,
    extract_bookplate_metadata,
    fetch_druids,
)

rows = Rows(
    DigitalBookplate(
        id=1,
        created=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        updated=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        druid="kp761xz4568",
        fund_name="ASHENR",
        image_filename="dp698zx8237_00_0001.jp2",
        title="Ruth Geraldine Ashen Memorial Book Fund",
    ),
    DigitalBookplate(
        id=2,
        created=datetime.datetime(2024, 9, 12, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 12, 17, 16, 15, 986798),
        druid="gc698jf6425",
        image_filename="gc698jf6425_00_0001.jp2",
        fund_name="RHOADES",
        title="John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
    ),
    DigitalBookplate(
        id=3,
        created=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        druid="ab123xy4567",
        fund_name=None,
        image_filename="ab123xy4567_00_0001.jp2",
        title="Alfred E. Newman Magazine Fund for Humor Studies",
    ),
)

engine = create_sqlite_fixture(rows)

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


@pytest.fixture
def pg_hook(mocker, engine) -> PostgresHook:
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


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
    assert metadata["image_filename"].startswith("future_library.jp2")


def test_failed_bookplate(pg_hook):
    failed_metadata = {
        "druid": "ef919yq2614",
        "failure": "Missing image file",
        "fund_name": "KELP",
        "title": "The Kelp Foundation Fund",
        "image_filename": None,
    }

    result = add_update_model.function(failed_metadata)

    assert result["failure"]["druid"] == "ef919yq2614"


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


def test_nochange_bookplate(pg_hook):
    metadata = {
        "druid": "gc698jf6425",
        "failure": None,
        "image_filename": "gc698jf6425_00_0001.jp2",
        "fund_name": "RHOADES",
        "title": "John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
    }

    result = add_update_model.function(metadata)
    assert result == {}


def test_new_bookplate(pg_hook):
    new_metadata = {
        "druid": "ef919yq2614",
        "failure": None,
        "fund_name": "KELP",
        "title": "The Kelp Foundation Fund",
        "image_filename": "ef919yq2614_00_0001.jp2",
    }

    result = add_update_model.function(new_metadata)

    assert result["new"]["db_id"] == 4


def test_update_bookplate(pg_hook):
    updated_metadata = {
        "druid": "ab123xy4567",
        "failure": None,
        "fund_name": "WHATMEWORRY",
        "title": "Alfred E. Newman Mad Magazine Fund for Humor Studies",
        "image_filename": "ab123xy4567_00_0001.jp2",
    }

    result = add_update_model.function(updated_metadata)

    assert result["updated"]["reason"].startswith("fund_name changed, title changed")
    assert result["updated"]["db_id"] == 3
