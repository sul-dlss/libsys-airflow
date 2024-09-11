import pytest  # noqa

from bs4 import BeautifulSoup
from airflow.models import Variable

from libsys_airflow.plugins.digital_bookplates.email import (
    bookplates_metadata_email,
)


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
