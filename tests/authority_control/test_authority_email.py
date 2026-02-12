import pytest


from airflow.sdk import Variable
from bs4 import BeautifulSoup
from pydantic import BaseModel

from libsys_airflow.plugins.authority_control.email import (
    email_deletes_report,
    email_report,
)


@pytest.fixture
def mock_folio_variables(monkeypatch):

    def mock_get(key, *args):
        value = None
        match key:
            case "OKAPI_URL":
                value = "folio-test"

            case "EMAIL_DEVS":
                value = "sul-unicorn-devs@lists.stanford.edu"

            case "FOLIO_URL":
                value = "https://folio-test.edu"

        return value

    monkeypatch.setattr(Variable, "get", mock_get)


class MockDag(BaseModel):
    dag_id: str = "load_marc_file"


class MockDagRun(BaseModel):
    run_id: str = "marc-import-2025-02-12T00:00:00+00:00"
    dag: MockDag = MockDag()


def test_email_deletes_report(mocker, mock_folio_variables):
    mock_send_email = mocker.MagicMock()

    mocker.patch.multiple(
        "libsys_airflow.plugins.shared.utils",
        send_email=mock_send_email,
        is_production=lambda: False,
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.is_production",
        return_value=False,
    )

    dag_run = MockDagRun()
    dag_run.run_id = "manual__2025-12-16T20:35:13"
    dag_run.dag.dag_id = "delete_authority_records"

    email_deletes_report(
        deleted=10,
        missing=["n79057860"],
        multiples=[],
        errors=[
            "n79059011: 500 Server Error",
            "92ca29a2-c4cc-47ad-bd0e-4b3150078b91: Error",
        ],
        dag_run=dag_run,
    )

    assert mock_send_email.call_count == 1

    report_body = BeautifulSoup(
        mock_send_email.call_args_list[0][1]['html_content'], 'html.parser'
    )
    title = report_body.find("h2")
    assert title.text.startswith("FOLIO Authority Record Deletion")
    all_paragraphs = report_body.find_all("p")
    assert all_paragraphs[0].text.startswith("DAG Run")
    all_subtitles = report_body.find_all("h3")
    assert all_subtitles[0].text.startswith("Successfully deleted 10")
    assert all_subtitles[1].text.startswith("No Match Found 1")
    assert all_subtitles[2].text.startswith("Multiple Matches Found 0")
    assert all_subtitles[3].text.startswith("Errors 2")


def test_email_report(mocker, mock_folio_variables):

    mock_send_email = mocker.MagicMock()

    mocker.patch.multiple(
        "libsys_airflow.plugins.shared.utils",
        send_email=mock_send_email,
        is_production=lambda: False,
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.is_production",
        return_value=False,
    )

    email_report(batch_report="Ran folio-data-import", dag_run=MockDagRun())

    assert mock_send_email.call_count == 1
