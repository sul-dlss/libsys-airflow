import datetime

import pytest
import requests


from bs4 import BeautifulSoup
from pytest_mock import MockerFixture

from libsys_airflow.plugins.folio.helpers.bw import (
    add_admin_notes,
    create_admin_note,
    create_bw_record,
    email_failure,
    email_bw_summary,
    post_bw_record,
)

from tests.mocks import (  # noqa
    mock_dag_run,
    MockFOLIOClient,
    MockTaskInstance,
)


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
        task_id = kwargs.get("task_ids")
        key = kwargs.get("key")
        match task_id:
            case "init_bw_relationships":
                match key:
                    case "file_name":
                        return "bw-test-file.csv"

                    case "user_email":
                        return "jstanford@example.com"

            case "new_bw_record":
                match key:
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
            query = kwargs.get("query_params").get("query")
            if "23456" in query:
                return []
            if "00032200" in query:
                return [{"id": "cc8dc750-3ca9-4cbc-a94a-709cd78d3d49"}]
        if args[0] == "/holdings-storage/holdings":
            query = kwargs.get("query_params").get("query")
            output = []
            if "ah1598042_1" in query:
                output.append({"id": "d32d28d3-e14c-4b69-8993-ce8640a91dc1"})
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
    mock_client.folio_get = mock_get
    mock_client.folio_put = mock_put
    mock_client.folio_post = mock_post
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


@pytest.fixture
def mock_context():
    return {
        "params": {
            "email": "jstanford@example.com",
            "file_name": "bw-errors.csv",
            "relationships": [
                {
                    'part_holdings_hrid': 'ah1135444_2',
                    'principle_barcode': '36105042288113',
                },
                {
                    'part_holdings_hrid': 'ah787483_1',
                    'principle_barcode': '36105010089238',
                },
                {
                    'part_holdings_hrid': 'ah1545187_1',
                    'principle_barcode': '36105042353974',
                },
                {
                    'part_holdings_hrid': 'ah1598042_1',
                    'principle_barcode': '36105042353974',
                },
            ],
        },
    }


def test_email_failure(mocker, mock_context, mock_task_instance):
    def mock_get(*args):
        response = mocker
        response.status_code = 200
        response.text = """<html>
        <pre>
        <code>
        [2023-12-12, 00:32:43 UTC] {standard_task_runner.py:57} INFO - Started process 15352 to run task
        [2023-12-12, 00:32:43 UTC] {standard_task_runner.py:85} INFO - Job 17319: Subtask add_bw_record
        [2023-12-12, 00:32:45 UTC] {taskinstance.py:1824} ERROR - Task failed with exception
        Traceback (most recent call last):
        File "/home/airflow/.local/lib/python3.10/site-packages/airflow/decorators/base.py", line 220, in execute
            return_value = super().execute(context)
        File "/home/airflow/.local/lib/python3.10/site-packages/airflow/operators/python.py", line 181, in execute
            return_value = self.execute_callable()
        File "/home/airflow/.local/lib/python3.10/site-packages/airflow/operators/python.py", line 198, in execute_callable
            return self.python_callable(*self.op_args, **self.op_kwargs)
        File "/opt/airflow/libsys_airflow/dags/new_boundwiths.py", line 59, in add_bw_record
            raise ValueError("Add bw records")
        ValueError: Add bw records
        [2023-12-12, 00:32:45 UTC] {taskinstance.py:1345} INFO - Marking task as FAILED. dag_id=add_bw_relationships
        </code>
        </pre>
        </html>"""
        return response

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.folio.helpers.bw.send_email_with_server_name"
    )
    mock_requests = mocker.patch("libsys_airflow.plugins.folio.helpers.bw.requests")
    mock_requests.get = mock_get
    mock_variable = mocker.patch("libsys_airflow.plugins.folio.helpers.bw.Variable")
    mock_variable.get = lambda _: 'libsys-lists@example.com'

    mock_context["task_instance"] = mock_task_instance

    email_failure(mock_context)
    assert mock_send_email.called

    assert mock_send_email.call_args[1]['to'] == [
        'libsys-lists@example.com',
        'jstanford@example.com',
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    h2 = html_body.find("h2")

    assert "Error with File bw-errors.csv" in h2.text

    code = html_body.find("code")

    assert "ValueError: Add bw records" in code.text


def test_email_failure_bad_log_url(mocker, mock_context, mock_task_instance):
    def mock_get(*args):
        response = mocker
        response.status_code = 500
        response.text = """Internal Server Error"""
        return response

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.folio.helpers.bw.send_email_with_server_name"
    )
    mock_requests = mocker.patch("libsys_airflow.plugins.folio.helpers.bw.requests")
    mock_requests.get = mock_get
    mock_variable = mocker.patch("libsys_airflow.plugins.folio.helpers.bw.Variable")
    mock_variable.get = lambda _: 'libsys-lists@example.com'

    mock_context["task_instance"] = mock_task_instance

    email_failure(mock_context)

    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    div = html_body.find("div")

    assert "Error Internal Server Error trying to retrieve log" in div.text


def test_email_failure_no_log(mocker, mock_context, mock_task_instance):
    def mock_get(*args):
        response = mocker
        response.status_code = 200
        response.text = """<div></div>"""
        return response

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.folio.helpers.bw.send_email_with_server_name"
    )
    mock_requests = mocker.patch("libsys_airflow.plugins.folio.helpers.bw.requests")
    mock_requests.get = mock_get
    mock_variable = mocker.patch("libsys_airflow.plugins.folio.helpers.bw.Variable")
    mock_variable.get = lambda _: 'libsys-lists@example.com'

    mock_context["task_instance"] = mock_task_instance

    email_failure(mock_context)

    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    div = html_body.find("div")

    assert "Could not extract log" in div.text


def test_email_bw_summary(mocker, mock_task_instance, mock_context):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.folio.helpers.bw.send_email_with_server_name"
    )

    email_bw_summary('libsys-lists@example.com', mock_task_instance)

    assert mock_send_email.called
    assert mock_send_email.call_args[1]['to'] == [
        'libsys-lists@example.com',
        'jstanford@example.com',
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    paragraphs = html_body.find_all("p")

    assert paragraphs[0].text == "1 boundwith relationships created"

    list_items = html_body.find_all("li")
    assert list_items[0].text.startswith("Parts Holding ID 59f2bc54")
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
