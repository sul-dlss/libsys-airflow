import pydantic
import pytest
import requests

from airflow.models import Variable
from pytest_mock import MockerFixture


@pytest.fixture
def mock_okapi_success(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201
        post_response.headers = { "x-okapi-token": "jwtOkapi0"}
        return post_response

    monkeypatch.setattr(requests, "post", mock_post)

@pytest.fixture
def mock_dag_run(mocker: MockerFixture):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-03-05"
    return dag_run

@pytest.fixture
def mock_okapi_variable(monkeypatch):
    def mock_get(key):
        return "https://okapi-folio.dev.edu"

    monkeypatch.setattr(Variable, "get", mock_get)

class MockFOLIOClient(pydantic.BaseModel):
    okapi_url: str = "https://okapi.edu/"


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = lambda *args, **kwargs: "a0token"  # noqa

class MockLibraryConfig(pydantic.BaseModel):
    iteration_identifier: str = "a-library-config"
    okapi_password: str = "43nmSUL"
    okapi_url: str = "https://okapi.edu/"
    okapi_username: str = "admin"
    tenant_id: str = "a_lib"
