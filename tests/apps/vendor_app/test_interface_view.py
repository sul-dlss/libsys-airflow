import os
from datetime import datetime, timedelta, date
import shutil

import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from sqlalchemy.orm import Session
from sqlalchemy import select
from dotenv import load_dotenv

from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)
from tests.airflow_client import test_airflow_client  # noqa: F401

load_dotenv()
now = datetime.now()

rows = Rows(
    # set up a vendor and its "interface"
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="375C6E33-2468-40BD-A5F2-73F82FE56DB0",
        vendor_code_from_folio="ACME",
        acquisitions_unit_from_folio="ACMEUNIT",
        has_active_vendor_interfaces=False,
        last_folio_update=now,
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
    # a file was fetched 10 days ago, and was loaded 9 days ago
    VendorFile(
        id=1,
        created=now - timedelta(days=10),
        updated=now - timedelta(days=10),
        vendor_interface_id=1,
        vendor_filename="acme-marc.dat",
        filesize=1234,
        vendor_timestamp=now - timedelta(days=30),
        loaded_timestamp=now - timedelta(days=8),
        archive_date=now - timedelta(days=9),
        status=FileStatus.loaded,
        dag_run_id="EB2FD649-2B0A-4671-9BD4-309BF5197870",
    ),
    # a file was fetched 8 days ago but errored during load 3 days ago
    VendorFile(
        id=2,
        created=now - timedelta(days=8),
        updated=now - timedelta(days=8),
        vendor_interface_id=1,
        vendor_filename="acme-bad-marc.dat",
        filesize=123456,
        vendor_timestamp=now - timedelta(days=30),
        loaded_timestamp=None,
        archive_date=now - timedelta(days=3),
        status=FileStatus.loading_error,
        dag_run_id="520F69D8-97BF-47CA-976A-F9CD4DB3FAD7",
    ),
    # a file was fetched .5 days ago, and is waiting to be loaded
    VendorFile(
        id=3,
        created=now - timedelta(days=0.5),
        updated=now - timedelta(days=0.5),
        vendor_interface_id=1,
        vendor_filename="acme-extra-strength-marc.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=12),
        loaded_timestamp=None,
        status=FileStatus.fetched,
    ),
    # a file was fetched 1 day ago, and is waiting to be loaded
    VendorFile(
        id=4,
        created=now - timedelta(days=1),
        updated=now - timedelta(days=1),
        vendor_interface_id=1,
        vendor_filename="acme-lite-marc.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        archive_date=now + timedelta(days=0.5),
        status=FileStatus.fetched,
    ),
    # a file was that failed to fetch right now
    VendorFile(
        id=5,
        created=now - timedelta(days=1),
        updated=now,
        vendor_interface_id=1,
        vendor_filename="acme-ftp-broken-marc.dat",
        filesize=24601,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        status=FileStatus.fetching_error,
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


def test_vendor_files(engine):  # noqa: F811
    with Session(engine) as session:
        interface = session.get(VendorInterface, 1)
        assert len(interface.vendor_files) == 5


def test_pending_files(engine):
    with Session(engine) as session:
        interface = session.get(VendorInterface, 1)
        pending = interface.pending_files
        assert len(pending) == 3
        assert [v.vendor_filename for v in pending] == [
            "acme-extra-strength-marc.dat",
            "acme-lite-marc.dat",
            "acme-ftp-broken-marc.dat",
        ]


def test_processed_files(engine):
    with Session(engine) as session:
        interface = session.get(VendorInterface, 1)
        processed = interface.processed_files
        assert len(processed) == 2
        assert [v.vendor_filename for v in processed] == [
            "acme-marc.dat",
            "acme-bad-marc.dat",
        ]


def test_interface_view(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
        )

        response = test_airflow_client.get('/vendor_management/interfaces/1')
        assert response.status_code == 200
        pending = response.html.find(id='pending-files')
        assert pending
        assert len(pending.find_all('tr')) == 3

        loaded = response.html.find(id='loaded-files')
        assert loaded
        assert len(loaded.find_all('tr')) == 2


@pytest.mark.skipif(
    os.environ.get("AIRFLOW_VAR_OKAPI_URL") is None, reason="No Folio Environment"
)
def test_interface_edit_view(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
        )
        response = test_airflow_client.get('/vendor_management/interfaces/1/edit')
        assert response.status_code == 200


def test_reload_file(test_airflow_client, mock_db, mocker):  # noqa: F811
    mock_trigger_dag = mocker.patch(
        'libsys_airflow.plugins.vendor_app.vendors.trigger_dag'
    )
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
        )
        response = test_airflow_client.post('/vendor_management/file/1/load')
        assert response.status_code == 302

    with Session(mock_db()) as session:
        vendor_file = session.get(VendorFile, 1)
        assert vendor_file.status == FileStatus.loading

    mock_trigger_dag.assert_called_once_with(
        'default_data_processor',
        conf={
            "filename": 'acme-marc.dat',
            "vendor_uuid": '375C6E33-2468-40BD-A5F2-73F82FE56DB0',
            "vendor_interface_uuid": '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
            "dataload_profile_uuid": 'A8635200-F876-46E0-ACF0-8E0EFA542A3F',
        },
    )


def test_upload_file(test_airflow_client, mock_db, tmp_path, mocker):  # noqa: F811
    mock_trigger_dag = mocker.patch(
        'libsys_airflow.plugins.vendor_app.vendors.trigger_dag'
    )
    mocker.patch(
        'libsys_airflow.plugins.vendor.paths.vendor_data_basepath',
        return_value=str(tmp_path),
    )
    today = date(2021, 1, 1)
    mock_date = mocker.patch('libsys_airflow.plugins.vendor.archive.date')
    mock_date.today.return_value = today

    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
        )
        response = test_airflow_client.post(
            '/vendor_management/interfaces/1/file',
            data={
                'file-upload': (
                    open('tests/vendor/0720230118.mrc', 'rb'),
                    'acme-extra-strength-marc.dat',
                    'application/octet-stream',
                )
            },
        )
        assert response.status_code == 302

        assert os.path.exists(
            os.path.join(
                tmp_path,
                'downloads/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E/acme-extra-strength-marc.dat',
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                'archive/20210101/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E/acme-extra-strength-marc.dat',
            )
        )

        vendor_file = session.scalars(
            select(VendorFile).where(
                VendorFile.vendor_filename == 'acme-extra-strength-marc.dat'
            )
        ).first()
        assert vendor_file
        assert vendor_file.vendor_interface_id == 1
        assert vendor_file.filesize == 35981
        assert vendor_file.status == FileStatus.uploaded
        assert vendor_file.archive_date == today

        mock_trigger_dag.assert_called_once_with(
            'default_data_processor',
            conf={
                "filename": 'acme-extra-strength-marc.dat',
                "vendor_uuid": '375C6E33-2468-40BD-A5F2-73F82FE56DB0',
                "vendor_interface_uuid": '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
                "dataload_profile_uuid": 'A8635200-F876-46E0-ACF0-8E0EFA542A3F',
            },
        )


def test_download_file(test_airflow_client, mock_db, tmp_path, mocker):  # noqa: F811
    mocker.patch(
        'libsys_airflow.plugins.vendor.paths.vendor_data_basepath',
        return_value=str(tmp_path),
    )
    path = os.path.join(
        tmp_path,
        f"archive/{(now - timedelta(days=9)).strftime('%Y%m%d')}/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E/acme-marc.dat",
    )
    os.makedirs(os.path.dirname(path))
    shutil.copyfile("tests/vendor/0720230118.mrc", path)

    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
        )

        response = test_airflow_client.get('/vendor_management/file/1/download')
        assert response.status_code == 200
        assert response.content_type == 'application/octet-stream'
        assert response.content_length == 35981
