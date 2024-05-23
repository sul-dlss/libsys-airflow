# Ignoring flake8 because of long lines in the HTML string.
# flake8: noqa
import pathlib
import shutil

import pymarc
import pytest

from pytest_mock_resources import create_sqlite_fixture, Rows
from datetime import date, datetime

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.emails import (
    files_fetched_email_task,
    file_loaded_email_task,
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


@pytest.fixture
def mock_folio_variables(monkeypatch):
    def mock_get(key):
        value = None
        match key:
            case "FOLIO_URL":
                value = "https://folio-stage.stanford.edu"

            case "VENDOR_LOADS_TO_EMAIL":
                value = "test@stanford.edu"

            case _:
                raise ValueError("")
        return value

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def marc_path(tmp_path):
    dest_filepath = tmp_path / "3820230411.mrc"
    shutil.copyfile("tests/vendor/0720230118.mrc", dest_filepath)
    return dest_filepath


def test_send_files_fetched_email(pg_hook, mocker, mock_folio_variables):
    mock_date = mocker.patch("libsys_airflow.plugins.vendor.emails.date")
    mock_date.today.return_value = date(2021, 1, 1)
    mock_send_email = mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")

    send_files_fetched_email(
        vendor_uuid='375C6E33-2468-40BD-A5F2-73F82FE56DB0',
        vendor_interface_name='Acme FTP',
        vendor_code='ACME',
        vendor_interface_uuid='140530EB-EE54-4302-81EE-D83B9DAC9B6E',
        vendor_interface_url='https://folio-stage.stanford.edu/vendor_management/interfaces/1',
        downloaded_files=['123456.mrc', '234567.mrc'],
        environment='development',
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - Daily Fetch Report (2021-01-01) [development]",
        """
        <h5>Acme FTP (ACME) - <a href="https://folio-stage.stanford.edu/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h5>

        <p>
            Files fetched:
            <ul>
            
                <li>123456.mrc</li>
            
                <li>234567.mrc</li>
            
            </ul>
        </p>
        """,
    )


def test_files_fetched_email_task(pg_hook, mocker, mock_folio_variables, caplog):
    mock_date = mocker.patch("libsys_airflow.plugins.vendor.emails.date")
    mock_date.today.return_value = date(2021, 1, 1)
    mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")

    files_fetched_email_task.function(
        [],
        {
            'vendor_uuid': '375C6E33-2468-40BD-A5F2-73F82FE56DB0',
            'vendor_interface_name': 'Acme FTP',
            'vendor_code': 'ACME',
            'vendor_interface_uuid': '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
            'environment': 'stage',
        },
    )

    assert "Skipping sending email since no files downloaded." in caplog.text


def test_send_file_loaded_bib_email_no_001s(
    pg_hook, mocker, mock_folio_variables, tmp_path
):
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.is_marc",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.conf.get",
        return_value="https://sul-libsys-airflow-stage.stanford.edu/",
    )
    mock_date = mocker.patch("libsys_airflow.plugins.vendor.emails.date")
    mock_date.today.return_value = date(2021, 1, 1)
    mock_send_email = mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag="245",
            indicators=[" ", " "],
            subfields=[
                pymarc.Subfield(code="a", value="Considerations for using MARC")
            ],
        )
    )

    marc_path = tmp_path / "123456.mrc"
    with marc_path.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    now = datetime.now()

    send_file_loaded_email(
        vendor_uuid='375C6E33-2468-40BD-A5F2-73F82FE56DB0',
        vendor_code='ACME',
        vendor_interface_name='Acme FTP',
        vendor_interface_uuid="140530EB-EE54-4302-81EE-D83B9DAC9B6E",
        job_execution_id='d7460945-6f0c-4e74-86c9-34a8438d652e',
        load_time=now,
        records_count=37,
        filename="123456.mrc",
        download_path=marc_path.parent,
        srs_stats={
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 1,
            'totalDiscardedEntities': 2,
            'totalErrors': 3,
        },
        instance_stats={
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 3,
            'totalDiscardedEntities': 1,
            'totalErrors': 2,
        },
        is_marc=True,
        double_zero_ones=[],
        environment='development',
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - (123456.mrc) - File Load Report [development]",
        f"""
        <h5>FOLIO Catalog MARC Load started on {now}</h5>
        <h6>Acme FTP (ACME) - <a href="https://sul-libsys-airflow-stage.stanford.edu/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h6>

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

        <h5>001 Values</h5>
        <p>No 001 fields</p>""",
    )


def test_send_file_loaded_bib_email_with_001s(
    pg_hook, mocker, mock_folio_variables, tmp_path
):
    mock_date = mocker.patch("libsys_airflow.plugins.vendor.emails.date")
    mock_date.today.return_value = date(2021, 1, 1)

    mock_send_email = mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.is_marc",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.marc.is_marc",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.conf.get",
        return_value="https://sul-libsys-airflow-stage.stanford.edu/",
    )
    now = datetime.now()

    marc_path = tmp_path / "123456.mrc"
    with marc_path.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for hrid in ['in00000023779', 'in00000023780', 'in00000023781']:
            record = pymarc.Record()
            record.add_field(pymarc.Field(tag='001', data=hrid))
            marc_writer.write(record)

    send_file_loaded_email(
        vendor_uuid='375C6E33-2468-40BD-A5F2-73F82FE56DB0',
        vendor_code='ACME',
        vendor_interface_name='Acme FTP',
        vendor_interface_uuid="140530EB-EE54-4302-81EE-D83B9DAC9B6E",
        job_execution_id='d7460945-6f0c-4e74-86c9-34a8438d652e',
        filename='123456.mrc',
        load_time=now,
        records_count=37,
        download_path=marc_path.parent,
        srs_stats={
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 1,
            'totalDiscardedEntities': 2,
            'totalErrors': 3,
        },
        instance_stats={
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 3,
            'totalDiscardedEntities': 1,
            'totalErrors': 2,
        },
        environment='development',
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - (123456.mrc) - File Load Report [development]",
        f"""
        <h5>FOLIO Catalog MARC Load started on {now}</h5>
        <h6>Acme FTP (ACME) - <a href="https://sul-libsys-airflow-stage.stanford.edu/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h6>

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

        <h5>001 Values</h5>
        <ul>
          <li>in00000023779</li>
          <li>in00000023780</li>
          <li>in00000023781</li>
          </ul>""",
    )


def test_send_file_loaded_edi_email(pg_hook, mocker, mock_folio_variables, tmp_path):
    mock_date = mocker.patch("libsys_airflow.plugins.vendor.emails.date")
    mock_date.today.return_value = date(2021, 1, 1)
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.is_marc",
        return_value=False,
    )
    mocker.patch("libsys_airflow.plugins.vendor.emails.invoice_count", return_value=37)
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.conf.get",
        return_value="https://sul-libsys-airflow-stage.stanford.edu/",
    )

    mock_send_email = mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")

    now = datetime.now()

    txt_path = tmp_path / "inv574076.edi.txt"
    txt_path.touch()

    send_file_loaded_email(
        vendor_uuid='375C6E33-2468-40BD-A5F2-73F82FE56DB0',
        vendor_code='ACME',
        vendor_interface_name='Acme FTP',
        vendor_interface_uuid="140530EB-EE54-4302-81EE-D83B9DAC9B6E",
        job_execution_id='d7460945-6f0c-4e74-86c9-34a8438d652e',
        download_path=txt_path.parent,
        filename="inv574076.edi.txt",
        load_time=now,
        srs_stats={
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 1,
            'totalDiscardedEntities': 2,
            'totalErrors': 3,
        },
        instance_stats={
            'totalCreatedEntities': 31,
            'totalUpdatedEntities': 3,
            'totalDiscardedEntities': 1,
            'totalErrors': 2,
        },
        double_zero_ones=[],
        environment='development',
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - (inv574076.edi.txt) - File Load Report [development]",
        f"""
        <h5>FOLIO Catalog EDI Load started on {now}</h5>
        <h6>Acme FTP (ACME) - <a href="https://sul-libsys-airflow-stage.stanford.edu/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h6>

        <p>Filename inv574076.edi.txt - https://folio-stage.stanford.edu/data-import/job-summary/d7460945-6f0c-4e74-86c9-34a8438d652e</p>
        <p>37 invoices read from EDI file.</p>
        <p>31 SRS records created</p>
        <p>2 Instance errors</p>
        """,
    )


def test_send_file_not_loaded_email(pg_hook, mocker, mock_folio_variables):
    mock_send_email = mocker.patch("libsys_airflow.plugins.vendor.emails.send_email")
    mocker.patch(
        "libsys_airflow.plugins.vendor.emails.conf.get",
        return_value="https://sul-libsys-airflow-stage.stanford.edu/",
    )

    send_file_not_loaded_email(
        vendor_uuid='375C6E33-2468-40BD-A5F2-73F82FE56DB0',
        vendor_interface_name='Acme FTP',
        vendor_code='ACME',
        vendor_interface_uuid='140530EB-EE54-4302-81EE-D83B9DAC9B6E',
        vendor_interface_url="https://sul-libsys-airflow-stage.stanford.edu/vendor_management/interfaces/1",
        filename='123456.mrc',
        environment='development',
    )

    mock_send_email.assert_called_once_with(
        'test@stanford.edu',
        "Acme FTP (ACME) - (123456.mrc) - File Processed [development]",
        f"""
        <h5>Acme FTP (ACME) - <a href="https://sul-libsys-airflow-stage.stanford.edu/vendor_management/interfaces/1">140530EB-EE54-4302-81EE-D83B9DAC9B6E</a></h5>

        <p>
            File processed, but not loaded: 123456.mrc
        </p>
        """,
    )
