import logging

from datetime import datetime
from typing import Optional, Callable

import pydantic
import pytest
import requests

from airflow.sdk import Variable
from pytest_mock import MockerFixture


logger = logging.getLogger(__name__)


@pytest.fixture
def mock_okapi_success(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        get_response.status_code = 200
        logger.info(f"Args:\n{args}")
        logger.info(f"Kwargs:\n{kwargs}")
        return get_response

    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201
        post_response.headers = {"x-okapi-token": "jwtOkapi0"}
        post_response.raise_for_status = lambda: None
        post_response_elapsed = mocker.stub(name="post_elapsed")
        post_response_elapsed.total_seconds = lambda: 30
        post_response.elapsed = post_response_elapsed
        post_response.text = ""
        return post_response

    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 200
        put_response.headers = {"x-okapi-token": "jwtOkapi0"}
        put_response.raise_for_status = lambda: None
        return put_response

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "put", mock_put)


@pytest.fixture
def mock_dag_run(mocker: MockerFixture):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-03-05"
    dag_run.start_date = datetime.now()

    return dag_run


@pytest.fixture
def mock_okapi_variable(monkeypatch):
    def mock_get(key):
        return "https://okapi-folio.dev.edu"

    monkeypatch.setattr(Variable, "get", mock_get)


# Mock xcom messages dict
messages: dict[str, str] = {}


# Mock xcoms
def mock_xcom_push(*args, **kwargs):
    task_instance = args[0]
    key = kwargs["key"]
    value = kwargs["value"]
    if task_instance.task_id in messages:
        messages[task_instance.task_id][key] = value
    else:
        messages[task_instance.task_id] = {key: value}


def mock_xcom_pull(*args, **kwargs):
    task_id = kwargs["task_ids"]
    key = kwargs.get("key")
    if task_id in messages:
        if key in messages[task_id]:
            return messages[task_id][key]
    return "a0token"


class MockFOLIOClient(pydantic.BaseModel):
    okapi_url: str = "https://okapi.edu"
    okapi_headers: dict = {}
    locations: list = []
    folio_get: Optional[Callable | None] = None


class MockTaskInstance(pydantic.BaseModel):
    task_id: str = "MockTaskInstance"
    xcom_pull = mock_xcom_pull
    xcom_push = mock_xcom_push


class MockLibraryConfig(pydantic.BaseModel):
    add_time_stamp_to_file_names: bool = False
    iteration_identifier: str = "a-library-config"
    log_level_debug: bool = False
    base_folder: str = "/opt/airflow/migration"
    okapi_password: str = "43nmSUL"
    okapi_url: str = "https://okapi.edu"
    okapi_username: str = "admin"
    tenant_id: str = "a_lib"
