import pytest  # noqa

from bs4 import BeautifulSoup

from libsys_airflow.plugins.data_exports.email import (
    generate_multiple_oclc_identifiers_email,
)


def test_multiple_oclc_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    generate_multiple_oclc_identifiers_email(
        [
            (
                "ae0b6949-6219-51cd-9a61-7794c2081fe7",
                "STF",
                ["(OCoLC-M)21184692", "(OCoLC-I)272673749"],
            ),
            (
                "0221724f-2bca-497b-8d42-6786295e7173",
                "HIN",
                ["(OCoLC-M)99087632", "(OCoLC-I)889220055"],
            ),
        ]
    )
    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    list_items = html_body.find_all("li")

    assert (
        list_items[0]
        .find("a")
        .get("href")
        .endswith("/inventory/viewsource/ae0b6949-6219-51cd-9a61-7794c2081fe7")
    )

    assert "(OCoLC-M)21184692" in list_items[0].text

    assert (
        list_items[1]
        .find("a")
        .get("href")
        .endswith("/inventory/viewsource/0221724f-2bca-497b-8d42-6786295e7173")
    )


def test_no_multiple_oclc_code_email(mocker, caplog):
    mocker.patch("libsys_airflow.plugins.data_exports.email.send_email")

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    generate_multiple_oclc_identifiers_email([])

    assert "No multiple OCLC Identifiers" in caplog.text
