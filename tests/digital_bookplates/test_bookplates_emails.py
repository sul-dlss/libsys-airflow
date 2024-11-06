import pytest  # noqa

from bs4 import BeautifulSoup
from airflow.models import Variable

from libsys_airflow.plugins.digital_bookplates.email import (
    bookplates_metadata_email,
    deleted_from_argo_email,
    missing_fields_email,
    summary_add_979_dag_runs,
)


failures = [
    {
        "druid": "cv850pd2472",
        "image_filename": None,
        "failure": "Missing image file",
        "fund_name": "ROSENBERG",
        "title": "Marie Louise Rosenberg Fund for Humanities",
    },
    {
        "druid": "fr739cb8411",
        "image_filename": "fr739cb8411_00_0001.jp2",
        "failure": "Missing title",
        "fund_name": None,
        "title": None,
    },
]


@pytest.fixture
def mock_folio_variables(monkeypatch):
    def mock_get(key, *args):
        value = None
        match key:
            case "FOLIO_URL":
                value = "folio-test"

            case "EMAIL_DEVS":
                value = "test@example.com"

            case "BOOKPLATES_EMAIL":
                value = "nobody@example.com"

            case "OKAPI_URL":
                value = "okapi-test"

            case _:
                raise ValueError("")
        return value

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_bookplate_metadata():
    return {
        "new": [
            {
                "fund_name": "ABBOTT",
                "druid": "ab123cd4567",
                "image_filename": "image_ab123cd4567.jp2",
                "title": "Title",
            }
        ],
        "updated": [
            {
                "fund_name": "ABBOTT",
                "druid": "ab123cd4567",
                "image_filename": "image_ab123cd4567.jp2",
                "title": "Changed Title",
                "reason": "title changed",
            }
        ],
        "failures": [],
    }


def test_bookplates_metadata_email(
    mocker, mock_bookplate_metadata, mock_folio_variables
):

    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    bookplates_metadata_email.function(
        new=mock_bookplate_metadata["new"], updated=mock_bookplate_metadata["updated"]
    )

    assert mock_send_email.called

    assert (
        mock_send_email.call_args[1]["subject"]
        == "folio-test - Digital bookplates new and updated metadata"
    )

    assert mock_send_email.call_args[1]["to"] == ["test@example.com"]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    headers = html_body.find_all("h2")
    assert headers[0].text == "New digital bookplates metadata"
    assert headers[1].text == "Updated digital bookplates metadata"

    tables = html_body.find_all("table")
    new_trs = tables[0].find_all("tr")
    assert len(new_trs) == 2
    assert new_trs[0].find_all("th")[3].text.startswith("Title")
    assert new_trs[1].find_all("td")[0].text.startswith("ABBOTT")
    assert new_trs[0].find_all("th")[2].text.startswith("Filename")
    assert new_trs[1].find_all("td")[2].text.startswith("image_ab123cd4567.jp2")

    updated_trs = tables[1].find_all("tr")
    assert len(updated_trs) == 2
    assert updated_trs[0].find_all("th")[4].text.startswith("Reason")
    assert updated_trs[1].find_all("td")[4].text.startswith("title change")


def test_bookplates_metadata_email_none(mocker, mock_folio_variables, caplog):
    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    bookplates_metadata_email.function(new=None, updated=None)

    assert mock_send_email.called

    assert "No new bookplate metadata to send in email" in caplog.text

    assert "No updated bookplate metadata to send in email" in caplog.text


def test_no_new_bookplates_metadata_email(
    mocker, mock_bookplate_metadata, mock_folio_variables, caplog
):

    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    bookplates_metadata_email.function(
        new=[], updated=mock_bookplate_metadata["updated"]
    )

    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    assert "No new bookplate metadata to send in email" in caplog.text

    assert html_body.find("p").text == "No new digital bookplates this run."

    headers = html_body.find_all("h2")
    assert headers[0].text == "New digital bookplates metadata"
    assert headers[1].text == "Updated digital bookplates metadata"

    tables = html_body.find_all("table")
    assert len(tables) == 1
    updated_trs = tables[0].find_all("tr")
    assert len(updated_trs) == 2
    assert updated_trs[0].find_all("th")[3].text.startswith("Title")
    assert updated_trs[1].find_all("td")[3].text.startswith("Changed Title")
    assert updated_trs[1].find_all("td")[2].text.startswith("image_ab123cd4567.jp2")


def test_no_updated_bookplates_metadata_email(
    mocker, mock_bookplate_metadata, mock_folio_variables, caplog
):

    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    bookplates_metadata_email.function(
        new=mock_bookplate_metadata["new"],
        updated=[],
    )

    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    assert "New bookplate metadata to send in email" in caplog.text
    assert "No updated bookplate metadata to send in email" in caplog.text

    assert (
        html_body.find("p").text == "No updated digital bookplates metadata this run."
    )

    tables = html_body.find_all("table")
    assert len(tables) == 1


def test_deleted_from_argo_email(mocker, mock_folio_variables):

    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    deleted_druids_info = [
        {"title": "The Happy Fund", "druid": "ab123xy4567", "fund_name": "HAPY"}
    ]
    deleted_from_argo_email.function(deleted_druids=deleted_druids_info)

    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    assert html_body.find("h2").text.startswith(
        "Deleted from Argo -- Digital Bookplates Druids"
    )
    assert (
        html_body.find("li").text
        == "Title: The Happy Fund, Fund name: HAPY, Druid: ab123xy4567"
    )


def test_deleted_from_argo_email_no_druids(mocker, caplog):
    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    deleted_from_argo_email.function(deleted_druids=[])

    assert mock_send_email.called is False
    assert "No Deleted Druids from Argo" in caplog.text


def test_deleted_from_argo_email_prod(mocker, mock_folio_variables, caplog):
    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    mocker.patch(
        "libsys_airflow.plugins.shared.utils.is_production",
        return_value=True,
    )

    deleted_from_argo_email.function(deleted_druids=["ab123xy4567"])

    assert mock_send_email.call_args[1]["subject"].startswith(
        "Deleted Druids from Argo for Digital bookplates"
    )


def test_missing_fields_email(mocker, mock_folio_variables):
    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    missing_fields_email.function(failures=failures)

    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    assert html_body.find("h2").text.startswith(
        "Missing Fields from Argo Digital Bookplates Druids"
    )

    list_items = html_body.find_all("li")

    assert list_items[0].text.startswith("Failure: Missing image file")
    assert "Druid: fr739cb8411" in list_items[1].text


def test_missing_fields_email_no_failures(mock_folio_variables, caplog):

    missing_fields_email.function(failures=[])

    assert "No missing fields" in caplog.text


def test_missing_fields_email_prod(mocker, mock_folio_variables):
    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    mocker.patch(
        "libsys_airflow.plugins.shared.utils.is_production",
        return_value=True,
    )

    missing_fields_email.function(failures=failures)

    assert mock_send_email.call_args[1]["subject"].startswith(
        "Missing Fields for Digital Bookplates"
    )


def test_summary_add_979_dag_runs(mocker, mock_folio_variables):
    mock_send_email = mocker.patch("libsys_airflow.plugins.shared.utils.send_email")

    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.conf.get",
        return_value="https://sul-libsys-airflow.stanford.edu",
    )

    dag_runs = {
        "manual__2024-10-20T02:00:00+00:00": {
            "state": "success",
            "instances": [
                {
                    "uuid": "fddf7e4c-161e-4ae8-baad-058288f63e17",
                    "funds": [
                        {
                            "name": "HOSKINS",
                            "title": "Janina Wojcicka Hoskins Book Fund for Polish Humanities",
                        }
                    ],
                },
                {
                    "uuid": "5394da8a-e503-424e-aca7-7ba73ddafc03",
                    "funds": [
                        {
                            "name": None,
                            "title": "David Jackman Jr. and Sally J. Jackman Endowed Book Fund for Archeology and Anthropology of Pre-Columbian Civilizations",
                        }
                    ],
                },
            ],
        }
    }
    summary_add_979_dag_runs.function(dag_runs=dag_runs, email="dscully@stanford.edu")

    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    links = html_body.find_all("a")

    assert len(links) == 3

    assert links[0].attrs['href'].startswith("https://sul-libsys-airflow.stanford.edu")
    assert links[1].attrs['href'].endswith("view/fddf7e4c-161e-4ae8-baad-058288f63e17")

    tds = html_body.find_all("td")
    assert tds[0].text == "HOSKINS"
    assert len(tds[2].text) == 0  # Tests blank fund name
    assert tds[3].text.startswith("David Jackman Jr. and Sally J. Jackman")


def test_summary_add_979_dag_runs_prod(mocker, mock_folio_variables):
    mock_send_email = mocker.MagicMock()

    mock_is_production = lambda: True  # noqa

    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.is_production",
        return_value=True,
    )

    mocker.patch.multiple(
        "libsys_airflow.plugins.shared.utils",
        send_email=mock_send_email,
        is_production=mock_is_production,
    )

    summary_add_979_dag_runs.function(dag_runs={}, email="dscully@stanford.edu")

    assert mock_send_email.call_args[1]["subject"].startswith(
        "Summary of Adding 979 fields to MARC Workflows"
    )

    assert mock_send_email.call_args[1]["to"] == [
        'test@example.com',
        'nobody@example.com',
        'dscully@stanford.edu',
    ]
