import json

import pytest
import pydantic
import requests

from pytest_mock import MockerFixture

from plugins.tests.mocks import mock_file_system  # noqa

from plugins.folio.remediate import handle_record_errors


def mock_folio_get(client, path):
    if path.endswith("a541bc9c-84c3-553a-b8be-ffab79b324cb"):
        return {"metadata": {"updatedDate": "2022-03-04T22:48:23.319"}}
    else:
        raise Exception("HTTP 404\nNot Found")


class MockFolioClient(pydantic.BaseModel):
    okapi_url = "https://okapi-folio.dev.edu"
    password = "abdccde"
    username = "folio_admin"
    folio_get = mock_folio_get
    okapi_headers = {}


@pytest.fixture
def mock_dag_run(mocker: MockerFixture):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual__2022-04-07"
    return dag_run


def test_with_no_error_files(mock_dag_run, caplog):

    handle_record_errors(
        dag_run=mock_dag_run,
        base="instance-storage",
        endpoint="/inventory/instances",
        folio_client=MockFolioClient(),
    )

    assert "Finished error handling for instance-storage errors" in caplog.text


# Instance JSON record
instance_error_record = {
    "classifications": [
        {
            "classificationNumber": "PZ3.V288 Sa",
            "classificationTypeId": "ce176ace-a53e-4b4d-aa89-725ed7b2edac",
        },
        {
            "classificationNumber": "PS3543.A5648 S3 1911",
            "classificationTypeId": "ce176ace-a53e-4b4d-aa89-725ed7b2edac",
        },
    ],
    "contributors": [
        {
            "contributorNameTypeId": "2b94c631-fca9-4892-a730-03ee529ffe2a",
            "contributorTypeId": "9f0a2cf0-7a9b-45a2-a403-f68d2850d07c",
            "contributorTypeText": "Contributor",
            "name": "Van Dyke, Henry, 1852-1933",
            "primary": True,
        },
        {
            "contributorNameTypeId": "2b94c631-fca9-4892-a730-03ee529ffe2a",
            "contributorTypeId": "9f0a2cf0-7a9b-45a2-a403-f68d2850d07c",
            "contributorTypeText": "Binding designer",
            "name": "Richardson, R. K., 1877-",
            "primary": False,
        },
        {
            "contributorNameTypeId": "2e48e713-17f3-4c13-a9f8-23845bb210aa",
            "contributorTypeId": "a60314d4-c3c6-4e29-92fa-86cc6ace4d56",
            "contributorTypeText": "Publisher",
            "name": "Charles Scribner's Sons",
            "primary": False,
        },
        {
            "contributorNameTypeId": "2e48e713-17f3-4c13-a9f8-23845bb210aa",
            "contributorTypeId": "02c1c664-1d71-4f7b-a656-1abf1209848f",
            "contributorTypeText": "Printer",
            "name": "Scribner Press",
            "primary": False,
        },
    ],
    "discoverySuppress": False,
    "hrid": "in00000900118",
    "id": "a541bc9c-84c3-553a-b8be-ffab79b324cb",
    "identifiers": [
        {
            "identifierTypeId": "c858e4f2-2b6b-4385-842b-60732ee14abb",
            "value": "11023057",
        },
        {
            "identifierTypeId": "7e591197-f335-4afb-bc6d-a6d76ca3bace",
            "value": "(SIRSI)a5720835",
        },
    ],
    "indexTitle": "Sad shepherd : a christmas story",
    "instanceFormatIds": [],
    "instanceTypeId": "30fffe0e-e985-4144-b2e2-1e8179bdb41f",
    "languages": ["eng"],
    "metadata": {
        "createdByUserId": "6fb5a144-e9e6-46e7-a939-0d5a0eddb325",
        "createdDate": "2022-03-29T22:48:23.319",
        "updatedByUserId": "6fb5a144-e9e6-46e7-a939-0d5a0eddb325",
        "updatedDate": "2022-03-29T22:48:23.319",
    },
    "modeOfIssuanceId": "9d18a02f-5897-4c31-9106-c9abb5c7ae8b",
    "notes": [
        {
            "instanceNoteTypeId": "6a2533a7-4de2-4e64-8466-074c2fa9308c",
            "note": "Verso of t.p.: Published October, 1911",
            "staffOnly": False,
        },
        {
            "instanceNoteTypeId": "6a2533a7-4de2-4e64-8466-074c2fa9308c",
            "note": "Verso of t.p.: Printer's device of The Scribner Press",
            "staffOnly": False,
        },
        {
            "instanceNoteTypeId": "6a2533a7-4de2-4e64-8466-074c2fa9308c",
            "note": "Frontispiece",
            "staffOnly": False,
        },
    ],
    "physicalDescriptions": [
        "[6], 56, [2] p. (first 2 p. and last 2 p. blank), "
        "[1] leaf of plates : ill. ; 20 cm."
    ],
    "publication": [
        {
            "dateOfPublication": "1911",
            "place": "New York",
            "publisher": "Charles Scribner's Sons",
        }
    ],
    "source": "MARC",
    "staffSuppress": False,
    "title": "The sad shepherd : a Christmas story / by Henry Van Dyke.",
}


@pytest.fixture
def mock_okapi_success(monkeypatch, mocker: MockerFixture):
    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 204

        return put_response

    monkeypatch.setattr(requests, "put", mock_put)


def test_with_instance_error_file(
    mock_file_system, mock_dag_run, mock_okapi_success, caplog  # noqa
):
    # Mock out instance errors
    airflow_path = mock_file_system[0]
    results_dir = mock_file_system[3]

    error_file = results_dir / "errors-instance-storage-422.json"

    error_file.write_text(f"{json.dumps(instance_error_record)}\n")

    handle_record_errors(
        dag_run=mock_dag_run,
        base="instance-storage",
        airflow=airflow_path,
        endpoint="/inventory/instances",
        folio_client=MockFolioClient(),
    )

    assert caplog.messages[0].startswith(f"Processing error file {error_file}")


@pytest.fixture
def mock_okapi(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201

        return post_response

    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 404

        return put_response

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "put", mock_put)


holdings_error_record = {
    "id": "83e201d2-5a67-5a64-87c1-a00d16d2fbed",
    "_version": 1,
    "hrid": "hld00000284771",
    "holdingsTypeId": "03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    "formerIds": ["a5522881"],
    "instanceId": "2c11d0ef-ce07-5b7e-8444-fad3f91b2cae",
    "permanentLocationId": "99063163-5ecd-46d1-bf75-899eaffc3b84",
    "electronicAccess": [],
    "callNumberTypeId": "95467209-6d7b-468b-94df-0f5d7ad2747d",
    "callNumber": "KF156 .A113 2003",
    "notes": [],
    "holdingsStatements": [],
    "holdingsStatementsForIndexes": [],
    "holdingsStatementsForSupplements": [],
    "statisticalCodeIds": [],
    "holdingsItems": [],
    "bareHoldingsItems": [],
    "metadata": {
        "createdDate": "2022-03-03T23:17:18.450+00:00",
        "createdByUserId": "60cfc6e2-0de4-4dc3-a947-3b7ae0056eb7",
        "updatedDate": "2022-03-03T23:17:18.450+00:00",
        "updatedByUserId": "60cfc6e2-0de4-4dc3-a947-3b7ae0056eb7",
    },
}


def test_new_holdings_in_error_file(
    mock_file_system, mock_dag_run, mock_okapi, caplog  # noqa
):
    airflow_path = mock_file_system[0]
    results_dir = mock_file_system[3]

    error_file = results_dir / "errors-holdings-storage-422-manual__2022-04-07.json"

    error_file.write_text(f"{json.dumps(holdings_error_record)}\n")

    folio_client = MockFolioClient()

    handle_record_errors(
        dag_run=mock_dag_run,
        base="holdings-storage",
        airflow=airflow_path,
        endpoint="/holdings-storage/holdings",
        folio_client=folio_client,
    )

    assert (
        f"Added {holdings_error_record['id']} to {folio_client.okapi_url}"
        in caplog.messages
    )


@pytest.fixture
def mock_okapi_server_error(monkeypatch, mocker: MockerFixture):
    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 500
        put_response.text = "Internal Server Error"

        return put_response

    monkeypatch.setattr(requests, "put", mock_put)


def test_okapi_put_server_error(mock_okapi_server_error, caplog):  # noqa
    folio_client = MockFolioClient()

    _post_or_put_record(
        holdings_error_record, "/holdings-storage/holdings", folio_client
    )

    assert (
        f"Failed to PUT {holdings_error_record['id']} - 500\nInternal Server Error"
        in caplog.messages
    )


@pytest.fixture
def mock_okapi_bad_post_request(monkeypatch, mocker: MockerFixture):
    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 404

        return put_response

    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 400
        post_response.text = "unable to add holding -- malformed JSON"

        return post_response

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "put", mock_put)


def test_okapi_post_bad_request(mock_okapi_bad_post_request, caplog):  # noqa
    folio_client = MockFolioClient()

    _post_or_put_record(
        holdings_error_record, "/holdings-storage/holdings", folio_client
    )

    assert (
        f"Failed to POST {holdings_error_record['id']} to {folio_client.okapi_url} - 400\nunable to add holding -- malformed JSON"
        in caplog.messages
    )


item_record = {
    "id": "1234abcde",
    "metadata": {"updatedDate": "2022-04-07T23:30:23.319"},
}


def test_missing_uuid(caplog):

    folio_client = MockFolioClient()

    result = _is_missing_or_outdated(item_record, "items-holdings/items", folio_client)

    assert result
    assert caplog.messages[0].startswith(
        f"{item_record['id']} not present in {folio_client.okapi_url}"
    )
