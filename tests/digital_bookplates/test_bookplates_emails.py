import pytest  # noqa

from bs4 import BeautifulSoup
from airflow.models import Variable

from libsys_airflow.plugins.digital_bookplates.email import (
    bookplates_metadata_email,
    deleted_from_argo_email,
    missing_fields_email,
)


failures = [
    {
        'druid': 'cv850pd2472',
        'image_filename': None,
        'failure': 'Missing image file',
        'fund_name': 'ROSENBERG',
        'title': 'Marie Louise Rosenberg Fund for Humanities',
    },
    {
        'druid': 'fr739cb8411',
        'image_filename': 'fr739cb8411_00_0001.jp2',
        'failure': 'Missing title',
        'fund_name': None,
        'title': None,
    },
]


@pytest.fixture
def mock_folio_variables(monkeypatch):
    def mock_get(key):
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
                "filename": "image_ab123cd4567.jp2",
                "title": "Title",
            }
        ],
        "updated": [
            {
                "fund_name": "ABBOTT",
                "druid": "ab123cd4567",
                "filename": "image_ab123cd4567.jp2",
                "title": "Changed Title",
                "reason": "title changed",
            }
        ],
    }


def test_bookplates_metadata_email(
    mocker, mock_bookplate_metadata, mock_folio_variables
):

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

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
    assert new_trs[0].find_all('th')[3].text.startswith("Title")
    assert new_trs[1].find_all('td')[0].text.startswith("ABBOTT")

    updated_trs = tables[1].find_all("tr")
    assert len(updated_trs) == 2
    assert updated_trs[0].find_all('th')[4].text.startswith("Reason")
    assert updated_trs[1].find_all('td')[4].text.startswith("title change")


def test_no_new_bookplates_metadata_email(
    mocker, mock_bookplate_metadata, mock_folio_variables, caplog
):

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

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
    assert updated_trs[0].find_all('th')[3].text.startswith("Title")
    assert updated_trs[1].find_all('td')[3].text.startswith("Changed Title")


def test_no_updated_bookplates_metadata_email(
    mocker, mock_bookplate_metadata, mock_folio_variables, caplog
):

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

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

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

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
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

    deleted_from_argo_email.function(deleted_druids=[])

    assert mock_send_email.called is False
    assert "No Deleted Druids from Argo" in caplog.text


def test_deleted_from_argo_email_prod(mocker, mock_folio_variables, caplog):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.is_production",
        return_value=True,
    )

    deleted_from_argo_email.function(deleted_druids=["ab123xy4567"])

    assert mock_send_email.call_args[1]["subject"].startswith(
        "Deleted Druids from Argo for Digital bookplates"
    )


def test_missing_fields_email(mocker, mock_folio_variables):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

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
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.send_email"
    )

    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.email.is_production",
        return_value=True,
    )

    missing_fields_email.function(failures=failures)

    assert mock_send_email.call_args[1]["subject"].startswith(
        "Missing Fields for Digital Bookplates"
    )
