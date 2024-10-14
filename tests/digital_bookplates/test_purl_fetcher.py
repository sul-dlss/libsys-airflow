import datetime

import httpx
import pytest
import requests_mock

from unittest.mock import MagicMock
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pytest_mock_resources import create_sqlite_fixture, Rows

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from libsys_airflow.plugins.digital_bookplates.purl_fetcher import (
    add_update_model,
    check_deleted_from_argo,
    extract_bookplate_metadata,
    fetch_druids,
    filter_updates_errors,
    trigger_instances_dag,
    _fetch_folio_fund_id,
)

rows = Rows(
    DigitalBookplate(
        id=1,
        created=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        updated=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        druid="kp761xz4568",
        fund_name="ASHENR",
        fund_uuid="b8932bcd-7498-4f7e-a598-de9010561e42",
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
        fund_uuid="3402d045-2788-46fe-8c49-d89860c1f701",
        title="John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
    ),
    DigitalBookplate(
        id=3,
        created=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        druid="ab123xy4567",
        fund_name=None,
        fund_uuid=None,
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
def mock_folio_client():
    def mock_get(*args, **kwargs):
        # Funds
        if args[0].startswith("/finance/funds"):
            if kwargs["query_params"]["query"] == "name==KELP":
                return [
                    {
                        "id": "f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4",
                        "name": "KELP",
                    }
                ]
            elif kwargs["query_params"]["query"] == "name==EMPTY_FUND":
                return []
            else:
                return [{"id": "abc123"}]

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    return mock_client


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


def test_check_deleted_from_argo(pg_hook):

    druid_list = ["kp761xz4568", "gc698jf6425"]

    deleted_from_argo = check_deleted_from_argo.function(druid_list)

    assert deleted_from_argo[0]['druid'].startswith("ab123xy4567")
    assert deleted_from_argo[0]['fund_name'] is None
    assert deleted_from_argo[0]['title'].startswith("Alfred E. Newman Magazine Fund")


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
        "fund_uuid": None,
        "title": "The Kelp Foundation Fund",
        "image_filename": None,
    }

    result = add_update_model.function(failed_metadata)

    assert result["failure"]["druid"] == "ef919yq2614"


def test_fetch_folio_fund_id(mocker, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher._folio_client",
        return_value=mock_folio_client,
    )

    fund_names = "EMPTY_FUND"

    assert _fetch_folio_fund_id(fund_names) is None


def test_filter_updates_errors():
    db_results = [{"failure": {}}, {"new": {}}, {"update": {}}]

    result = filter_updates_errors.function(db_results)

    assert result['failures'] == [{}]


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


def test_new_bookplate(pg_hook, mocker, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher._folio_client",
        return_value=mock_folio_client,
    )

    new_metadata = {
        "druid": "ef919yq2614",
        "failure": None,
        "fund_name": "KELP",
        "title": "The Kelp Foundation Fund",
        "image_filename": "ef919yq2614_00_0001.jp2",
    }

    result = add_update_model.function(new_metadata)

    assert result["new"]["db_id"] == 4
    assert result["new"]["fund_uuid"] == "f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4"


def test_trigger_instances_dag_no_new(caplog):
    trigger_instances_dag.function(new=[])

    assert "No new funds to trigger digital_bookplate_instances DAG" in caplog.text


def test_update_bookplate(pg_hook, mocker, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.purl_fetcher._folio_client",
        return_value=mock_folio_client,
    )

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
    assert result["updated"]["fund_uuid"] == "abc123"
