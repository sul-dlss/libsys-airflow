import datetime
import json

import pytest
import requests


from bs4 import BeautifulSoup
from pytest_mock import MockerFixture

from libsys_airflow.plugins.folio.helpers.bw import (
    add_admin_notes,
    check_add_bw,
    create_admin_note,
    create_bw_record,
    discover_bw_parts_files,
    email_bw_summary,
    post_bw_record,
)

from tests.mocks import (  # noqa
    mock_file_system,
    mock_dag_run,
    MockFOLIOClient,
    MockTaskInstance,
)

import tests.mocks as mocks


@pytest.fixture
def mock_okapi_boundwith(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        if kwargs["json"]["id"] == "70980bf9-40bc-583a-8590-49b520b58b9f":
            post_response.status_code = 422
            post_response.text = """
            "errors" : [ {
    "message" : "Cannot set bound_with_part.holdingsrecordid = d89b35ba-27c9-5ba0-8c3d-9cae9eb2e9f4 because it does not exist in holdings_record.id.",
    "type" : "1",
    "code" : "-1",
    "parameters" : [ {
      "key" : "bound_with_part.holdingsrecordid",
      "value" : "d89b35ba-27c9-5ba0-8c3d-9cae9eb2e9f4"
    } ]
  } ]
}"""
        else:
            post_response.status_code = 201
        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.fixture
def mock_task_instance(mocker):
    messages = {}

    def mock_xcom_pull(**kwargs):
        match kwargs.get("key"):
            case "success":
                return [{"holdingsRecordId": "", "itemId": ""}]

            case "error":
                return [
                    {
                        "message": "Aleady exists",
                        "record": {
                            "holdingsRecordId": "59f2bc54-793e-426b-8087-632c2a3430a7",
                            "itemId": "4a5944f7-9d9f-427e-a510-3af7856241de",
                        },
                    }
                ]

    def mock_xcom_push(**kwargs):
        if mock_ti.task_ids in messages:
            messages[mock_ti.task_ids][kwargs["key"]] = kwargs["value"]
        else:
            messages[mock_ti.task_ids] = {kwargs["key"]: kwargs["value"]}

    mock_ti = mocker.MagicMock()
    mock_ti.task_ids = None
    mock_ti.xcom_pull = mock_xcom_pull
    mock_ti.xcom_push = mock_xcom_push
    mock_ti.messages = messages
    return mock_ti


@pytest.fixture
def mock_folio_client(mocker):
    def mock_get(*args, **kwargs):
        if args[0] == "/inventory/items":
            query = kwargs.get("params").get("query")
            if "23456" in query:
                return {"items": []}
            if "00032200" in query:
                return {"items": [{"id": "cc8dc750-3ca9-4cbc-a94a-709cd78d3d49"}]}
        if args[0] == "/holdings-storage/holdings":
            query = kwargs.get("params").get("query")
            output = {"holdingsRecords": []}
            if "ah1598042_1" in query:
                output["holdingsRecords"].append(
                    {"id": "d32d28d3-e14c-4b69-8993-ce8640a91dc1"}
                )
            return output
        return {"administrativeNotes": []}

    def mock_put(*args, **kwargs):
        return {}

    def mock_post(*args, **kwargs):
        if (
            kwargs['payload']['holdingsRecordId']
            == "17ce339c-2277-4f25-bdb1-e75f8cc00b0e"
        ):
            raise requests.exceptions.HTTPError("500 Invalid payload")
        return {"id": "71bd1912-d0e0-4de5-831a-1615affe3e81"}

    mock_client = mocker.MagicMock()
    mock_client.get = mock_get
    mock_client.put = mock_put
    mock_client.post = mock_post
    return mock_client


def test_add_admin_notes(mock_task_instance, mock_folio_client, caplog):
    add_admin_notes(
        "SUL/DLSS/LibrarySystems/BWcreatedby/jstanford/20231201",
        mock_task_instance,
        mock_folio_client,
    )

    assert "Total 1 Item/Holding pairs administrative notes"


def test_create_admin_note():
    note = create_admin_note("jstanford")
    date = datetime.datetime.utcnow().strftime("%Y%m%d")
    assert note == f"SUL/DLSS/LibrarySystems/BWcreatedby/jstanford/{date}"


def test_create_bw_record(mock_folio_client):
    bw_parts = create_bw_record(
        folio_client=mock_folio_client,
        holdings_hrid="ah1598042_1",
        barcode="00032200",
    )

    assert bw_parts["holdingsRecordId"] == "d32d28d3-e14c-4b69-8993-ce8640a91dc1"
    assert bw_parts["itemId"] == "cc8dc750-3ca9-4cbc-a94a-709cd78d3d49"


def test_create_bw_record_no_item(mock_folio_client):
    bw_parts = create_bw_record(
        folio_client=mock_folio_client,
        holdings_hrid="ah1598042_1",
        barcode="23456",
    )

    assert bw_parts == {}


def test_create_bw_record_no_holdings(mock_folio_client):
    bw_parts = create_bw_record(
        folio_client=mock_folio_client,
        holdings_hrid="ah1598042_2",
        barcode="23456",
    )

    assert bw_parts == {}


def test_check_add_bw(mock_file_system, mock_okapi_boundwith, caplog):  # noqa
    results_dir = mock_file_system[3]

    boundwith_file = results_dir / "boundwith_parts.json"
    with boundwith_file.open("w+") as fo:
        for row in [
            {
                "id": "6fb73520-ea6b-5f43-a6c7-7af9063fe439",
                "holdingsRecordId": "aa56077c-b252-53a0-9f6c-de9d209566c4",
                "itemId": "0c8dd766-bdbd-52fc-afbd-c4a9a69c4ac4",
            },
            {
                "id": "70980bf9-40bc-583a-8590-49b520b58b9f",
                "holdingsRecordId": "d89b35ba-27c9-5ba0-8c3d-9cae9eb2e9f4",
                "itemId": "8a840385-5eb3-51d9-bf46-8997732c4f61",
            },
        ]:
            fo.write(f"{json.dumps(row)}\n")

    mocks.messages["discovery-bw-parts"] = {"job-1": [str(boundwith_file)]}

    check_add_bw(
        task_instance=MockTaskInstance(), folio_client=MockFOLIOClient(), job=1
    )

    assert (
        "Cannot set bound_with_part.holdingsrecordid = d89b35ba-27c9-5ba0-8c3d-9cae9eb2e9f4"
        in caplog.text
    )
    assert "Finished added 2 boundwidths with 1 errors" in caplog.text
    mocks.messages = {}


def test_discover_bw_parts_files(mock_file_system, caplog):  # noqa
    airflow = mock_file_system[0]

    second_iteration = airflow / "migration/iterations/manual__2023-02-24"

    second_iteration.mkdir(parents=True)

    boundwith_file = mock_file_system[3] / "boundwith_parts.json"

    with boundwith_file.open("w+") as fo:
        record = {
            "id": "6fb73520-ea6b-5f43-a6c7-7af9063fe439",
            "holdingsRecordId": "aa56077c-b252-53a0-9f6c-de9d209566c4",
            "itemId": "0c8dd766-bdbd-52fc-afbd-c4a9a69c4ac4",
        }
        fo.write(f"{json.dumps(record)}\n")

    discover_bw_parts_files(
        airflow=airflow,
        jobs=2,
        task_instance=MockTaskInstance(task_id="discovery-bw-parts"),
    )
    assert len(mocks.messages["discovery-bw-parts"]["job-0"]) == 0
    assert mocks.messages["discovery-bw-parts"]["job-1"] == [str(boundwith_file)]

    assert (
        "manual__2023-02-24/results/boundwith_parts.json doesn't exist" in caplog.text
    )
    assert "Discovered 1 boundwidth part files for processing" in caplog.text

    mocks.messages = {}


def test_email_bw_summary(mocker, mock_task_instance):
    mock_send_email = mocker.patch("libsys_airflow.plugins.folio.helpers.bw.send_email")

    email_bw_summary(
        'jstanford@stanford.edu', 'libsys-lists@stanford.edu', mock_task_instance
    )

    assert mock_send_email.called
    assert mock_send_email.call_args[1]['to'] == [
        'libsys-lists@stanford.edu',
        'jstanford@stanford.edu',
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    paragraphs = html_body.find_all("p")

    assert paragraphs[0].text == "1 boundwith relationships created"

    list_items = html_body.find_all("li")
    assert list_items[0].text.startswith("Child Holding ID 59f2bc54")
    assert list_items[1].text.endswith("Item ID 4a5944f7-9d9f-427e-a510-3af7856241de")


def test_post_bw_record(mock_folio_client, mock_task_instance):
    mock_task_instance.task_ids = "new_bw_record"
    post_bw_record(
        folio_client=mock_folio_client,
        task_instance=mock_task_instance,
        bw_parts={
            "holdingsRecordId": "aa56077c-b252-53a0-9f6c-de9d209566c4",
            "itemId": "0c8dd766-bdbd-52fc-afbd-c4a9a69c4ac4",
        },
    )
    assert (
        mock_task_instance.messages["new_bw_record"]["success"]["id"]
        == "71bd1912-d0e0-4de5-831a-1615affe3e81"
    )


def test_post_bw_record_exception(mock_folio_client, mock_task_instance):
    mock_task_instance.task_ids = "new_bw_record"

    post_bw_record(
        folio_client=mock_folio_client,
        task_instance=mock_task_instance,
        bw_parts={
            "holdingsRecordId": "17ce339c-2277-4f25-bdb1-e75f8cc00b0e",
            "itemId": "0c8dd766-bdbd-52fc-afbd-c4a9a69c4ac4",
        },
    )
    assert (
        mock_task_instance.messages["new_bw_record"]["error"]["message"]
        == "500 Invalid payload"
    )
