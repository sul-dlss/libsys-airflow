import json

import pytest
import requests

from pytest_mock import MockerFixture

from libsys_airflow.plugins.folio.helpers.bw import discover_bw_parts_files, check_add_bw
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
        task_instance=MockTaskInstance(task_id="discovery-bw-parts"))
    assert len(mocks.messages["discovery-bw-parts"]["job-0"]) == 0
    assert mocks.messages["discovery-bw-parts"]["job-1"] == [str(boundwith_file)]

    assert (
        "manual__2023-02-24/results/boundwith_parts.json doesn't exist" in caplog.text
    )
    assert "Discovered 1 boundwidth part files for processing" in caplog.text

    mocks.messages = {}
