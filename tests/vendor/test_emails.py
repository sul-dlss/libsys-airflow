# Ignoring flake8 because of long lines in the HTML string.
# flake8: noqa
import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.emails import send_files_fetched_email
from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
)


rows = Rows(
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="375C6E33-2468-40BD-A5F2-73F82FE56DB0",
        vendor_code_from_folio="ACME",
        acquisitions_unit_from_folio="ACMEUNIT",
        last_folio_update=datetime.utcnow(),
    ),
    VendorInterface(
        id=1,
        display_name="Acme FTP",
        vendor_id=1,
        folio_interface_uuid="140530EB-EE54-4302-81EE-D83B9DAC9B6E",
        folio_data_import_processing_name="Acme Profile 1",
        folio_data_import_profile_uuid="A8635200-F876-46E0-ACF0-8E0EFA542A3F",
        file_pattern="*.mrc",
        remote_path="stanford/outgoing/data",
        processing_dag="acme-pull",
        processing_delay_in_days=3,
        active=True,
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def pg_hook(mocker, engine) -> PostgresHook:
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


def test_send_files_fetched_email(pg_hook, mocker):
    mock_date = mocker.patch("libsys_airflow.plugins.vendor.emails.date")
    mock_date.today.return_value = date(2021, 1, 1)
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.os.getenv",
        return_value="test@stanford.edu",
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.conf.get",
        return_value="https://www.example.com",
    )
    mock_send_email = mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")

    send_files_fetched_email(
        'Acme',
        'ACME',
        '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
        ['123456.mrc', '234567.mrc'],
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme (ACME) - 140530EB-EE54-4302-81EE-D83B9DAC9B6E - Daily Fetch Report (2021-01-01)",
        """
        <h5>Acme (ACME) - <a href="https://www.example.com/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h5>

        <p>
            Files fetched:
            <ul>
            
                <li>123456.mrc</li>
            
                <li>234567.mrc</li>
            
            </ul>
        </p>
        """,
    )
