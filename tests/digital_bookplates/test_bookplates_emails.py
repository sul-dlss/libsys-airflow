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
        "new": ["Fund name\tab123cd4567\timage_ab123cd4567.jp2\tTitle"],
        "updated": ["Fund name\tab123cd4567\timage_ab123cd4567.jp2\tChanged Title"],
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

    list_items = html_body.find_all("li")
    assert (
        list_items[0].text.strip()
        == "Fund name\tab123cd4567\timage_ab123cd4567.jp2\tTitle"
    )
    assert (
        list_items[1].text.strip()
        == "Fund name\tab123cd4567\timage_ab123cd4567.jp2\tChanged Title"
    )


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

    list_items = html_body.find_all("li")
    assert (
        list_items[0].text.strip()
        == "Fund name\tab123cd4567\timage_ab123cd4567.jp2\tChanged Title"
    )


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

    list_items = html_body.find_all("li")
    assert (
        list_items[0].text.strip()
        == "Fund name\tab123cd4567\timage_ab123cd4567.jp2\tTitle"
    )
