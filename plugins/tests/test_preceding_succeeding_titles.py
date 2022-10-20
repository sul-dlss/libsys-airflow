from unittest.mock import MagicMock
import requests
import pytest


from pytest_mock import MockerFixture
from dags.preceding_succeding_titles import post_to_folio
from plugins.folio import helpers
from plugins.tests.mocks import (  # noqa
    MockTaskInstance,
    mock_file_system,
    mock_dag_run,
    mock_okapi_variable,
)


@pytest.fixture
def mock_okapi_post_error(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 400
        post_response.json = lambda: {"errors": [{"message": "somme error"}]}
        post_response.text = "error"
        return post_response

    monkeypatch.setattr(requests, "post", mock_post)

    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 200
        put_response.headers = {"x-okapi-token": "jwtOkapi0"}
        put_response.raise_for_status = lambda: None
        return put_response

    monkeypatch.setattr(requests, "put", mock_put)


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {"iteration_id": "manual_2022-03-05"}
        return context

    monkeypatch.setattr(
        "dags.preceding_succeding_titles.get_current_context", mock_get_current_context
    )


def test_preceding_succeeding_titles(
    mock_file_system,  # noqa
    mock_okapi_post_error,
    mock_okapi_variable,  # noqa
    mock_get_current_context,
    caplog,  # noqa
):
    airflow = mock_file_system[0]
    results_dir = mock_file_system[3]

    helpers._save_error_record_ids = MagicMock(airflow=results_dir)

    # Create mock JSON file
    titles_filename = "preceding_succeeding_titles.json"
    titles_file = results_dir / titles_filename
    titles_file.write_text(
        """{ "id": "11111111-1111-1111-1111-111111111111", "title": "Preceding Title", "identifiers": [], "succeedingInstanceId": "22222222-2222-2222-2222-222222222222" }"""
    )

    mock_task_instance = MockTaskInstance
    post_to_folio(
        airflow=str(airflow),
        dag_run=mock_dag_run,
        task_instance=mock_task_instance,
    )

    assert "trying a PUT instead" in caplog.text
