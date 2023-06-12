# Ignoring flake8 because of long lines in the HTML string.
# flake8: noqa
import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from datetime import date, datetime

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.emails import (
    send_files_fetched_email,
    send_file_loaded_email,
    send_file_not_loaded_email,
)
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
        'Acme FTP',
        'ACME',
        '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
        ['123456.mrc', '234567.mrc'],
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - 140530EB-EE54-4302-81EE-D83B9DAC9B6E - Daily Fetch Report (2021-01-01)",
        """
        <h5>Acme FTP (ACME) - <a href="https://www.example.com/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h5>

        <p>
            Files fetched:
            <ul>
            
                <li>123456.mrc</li>
            
                <li>234567.mrc</li>
            
            </ul>
        </p>
        """,
    )


@pytest.fixture
def mock_okapi_url_variable(monkeypatch):
    def mock_get(key):
        return "https://folio-stage.stanford.edu"

    monkeypatch.setattr(Variable, "get", mock_get)


def test_send_file_loaded_bib_email(pg_hook, mocker, mock_okapi_url_variable):
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

    now = datetime.now()

    send_file_loaded_email(
        'ACME',
        'Acme FTP',
        'https://folio-stage.stanford.edu/data-import/job-summary/d7460945-6f0c-4e74-86c9-34a8438d652e',
        '123456.mrc',
        now,
        37,
        {
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 1,
            'totalDiscardedEntities': 2,
            'totalErrors': 3,
        },
        {
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 3,
            'totalDiscardedEntities': 1,
            'totalErrors': 2,
        },
        True,
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - (123456.mrc) - File Load Report",
        f"""
        <h5>FOLIO Catalog MARC Load started on {now}</h5>

        <p>Filename 123456.mrc - https://folio-stage.stanford.edu/data-import/job-summary/d7460945-6f0c-4e74-86c9-34a8438d652e</p>
        <p>37 bib record(s) read from MARC file.</p>
        <p>31 SRS records created</p>
        <p>1 SRS records updated</p>
        <p>2 SRS records discarded</p>
        <p>3 SRS errors</p>
        <p>31 Instance records created</p>
        <p>3 Instance records updated</p>
        <p>1 Instance records discarded</p>
        <p>2 Instance errors</p>
        """,
    )


def test_send_file_loaded_edi_email(pg_hook, mocker, mock_okapi_url_variable):
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

    now = datetime.now()

    send_file_loaded_email(
        'ACME',
        'Acme FTP',
        'https://folio-stage.stanford.edu/data-import/job-summary/d7460945-6f0c-4e74-86c9-34a8438d652e',
        '123456.mrc',
        now,
        37,
        {
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 1,
            'totalDiscardedEntities': 2,
            'totalErrors': 3,
        },
        {
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 3,
            'totalDiscardedEntities': 1,
            'totalErrors': 2,
        },
        False,
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - (123456.mrc) - File Load Report",
        f"""
        <h5>FOLIO Catalog EDI Load started on {now}</h5>

        <p>Filename 123456.mrc - https://folio-stage.stanford.edu/data-import/job-summary/d7460945-6f0c-4e74-86c9-34a8438d652e</p>
        <p>37 invoices read from EDI file.</p>
        <p>31 SRS records created</p>
        <p>2 Instance errors</p>
        """,
    )


def test_send_file_not_loaded_email(pg_hook, mocker):
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.os.getenv",
        return_value="test@stanford.edu",
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.conf.get",
        return_value="https://www.example.com",
    )
    mock_send_email = mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")

    send_file_not_loaded_email(
        'Acme FTP',
        'ACME',
        '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
        '123456.mrc',
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - (123456.mrc) - File Processed",
        f"""
        <h5>Acme FTP (ACME) - <a href="https://www.example.com/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h5>

        <p>
            File processed, but not loaded: 123456.mrc
        </p>
        """,
    )
