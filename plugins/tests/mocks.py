import logging

import pydantic
import pytest
import requests

from airflow.models import Variable
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
    return dag_run


@pytest.fixture
def mock_okapi_variable(monkeypatch):
    def mock_get(key):
        return "https://okapi-folio.dev.edu"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_file_system(tmp_path, mock_dag_run):
    airflow_path = tmp_path / "opt/airflow/"

    # Mock source and target dirs
    source_dir = airflow_path / "symphony"
    source_dir.mkdir(parents=True)

    sample_marc = source_dir / "sample.mrc"
    sample_marc.write_text("sample")

    iteration_dir = airflow_path / f"migration/iterations/{mock_dag_run.run_id}"
    iteration_dir.mkdir(parents=True)

    # Mock plugins directory
    config_dir = airflow_path / "plugins/folio"
    config_dir.mkdir(parents=True)

    # Makes directories for different type of data for mock_dag_run
    source_data = iteration_dir / "source_data"
    (source_data / "instances").mkdir(parents=True)
    (source_data / "holdings").mkdir(parents=True)
    (source_data / "items").mkdir(parents=True)
    (source_data / "users").mkdir(parents=True)

    # Mock Results, Reports, Mapping Files, and Archive Directories
    results_dir = iteration_dir / "results"
    results_dir.mkdir(parents=True)
    (iteration_dir / "reports").mkdir(parents=True)
    (airflow_path / "migration/mapping_files").mkdir(parents=True)
    archive_dir = iteration_dir / "archive"

    # Mock .gitignore
    gitignore = airflow_path / "migration/.gitignore"
    gitignore.write_text("results\nreports")

    # mock tmp dir
    tmp = tmp_path / "tmp/"
    tmp.mkdir(parents=True)

    return [airflow_path, source_dir, iteration_dir, results_dir, archive_dir, tmp]


class MockFOLIOClient(pydantic.BaseModel):
    okapi_url: str = "https://okapi.edu"
    okapi_headers: dict = {}


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = lambda *args, **kwargs: "a0token"  # noqa


class MockLibraryConfig(pydantic.BaseModel):
    add_time_stamp_to_file_names: bool = False
    iteration_identifier: str = "a-library-config"
    log_level_debug: bool = False
    base_folder: str = "/opt/airflow/migration"
    okapi_password: str = "43nmSUL"
    okapi_url: str = "https://okapi.edu"
    okapi_username: str = "admin"
    tenant_id: str = "a_lib"
