from datetime import datetime, timedelta, date
import shutil
from unittest.mock import MagicMock, PropertyMock

import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from sqlalchemy.orm import Session
from sqlalchemy import select

from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)
from tests.airflow_client import test_airflow_client  # noqa: F401

now = datetime.utcnow()

rows = Rows(
    # set up a vendor and its "interface"
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="375C6E33-2468-40BD-A5F2-73F82FE56DB0",
        vendor_code_from_folio="ACME",
        acquisitions_unit_from_folio="ACMEUNIT",
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
        note="A note about Acme FTP Interface",
        remote_path="stanford/outgoing/data",
        processing_dag="acme-pull",
        processing_options={
            "package_name": "Acme ebooks package",
            "prepend_001": {"tag": "001", "data": "eb4"},
            "delete_marc": ["666", "667"],
            "add_subfield": [
                {
                    "tag": "856",
                    "eval_subfield": "u",
                    "pattern": "^http:\\/\\/ebooks\\.acme\\.com.+",
                    "subfields": [{"code": "x", "value": "eb4"}],
                }
            ],
            "change_marc": [
                {
                    "from": {
                        "tag": "856",
                        "indicator1": "4",
                        "indicator2": "1",
                    },
                    "to": {
                        "tag": "856",
                        "indicator1": "4",
                        "indicator2": "0",
                    },
                },
            ],
        },
        processing_delay_in_days=3,
        active=True,
    ),
    VendorInterface(id=2, display_name="Acme Upload Only", vendor_id=1, active=True),
    # a file was fetched 10 days ago, and was loaded 9 days ago
    VendorFile(
        id=1,
        created=now - timedelta(days=10),
        updated=now - timedelta(days=10),
        vendor_interface_id=1,
        vendor_filename="acme-marc.dat",
        processed_filename="acme-marc-processed.dat",
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


@pytest.fixture
def job_profiles():
    job_profiles_resp = {
        'jobProfiles': [
            {
                'id': '6409dcff-71fa-433a-bc6a-e70ad38a9604',
                'name': 'Some job profile',
                'description': 'This job profile is used to create a new SRS MARC Bib record',
                'dataType': 'MARC',
                'deleted': False,
            },
        ],
        'totalRecords': 1,
    }
    return job_profiles_resp


@pytest.fixture
def mock_dag_run():
    mock_dag_run = MagicMock()
    type(mock_dag_run).execution_date = PropertyMock(
        return_value=datetime(2023, 4, 25, 16, 34, 12, 777715)
    )
    type(mock_dag_run).run_id = PropertyMock(
        return_value='manual__2023-04-25T16:34:12.777715+00:00'
    )
    return mock_dag_run


@pytest.fixture
def mock_variable(mocker):
    return mocker.patch(
        'libsys_airflow.plugins.vendor_app.vendor_management.Variable.get',
        return_value='https://folio-stage.edu/',
    )


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
            "acme-ftp-broken-marc.dat",
            "acme-extra-strength-marc.dat",
            "acme-lite-marc.dat",
        ]


def test_processed_files(engine):
    with Session(engine) as session:
        interface = session.get(VendorInterface, 1)
        processed = interface.processed_files
        assert len(processed) == 2
        assert [v.vendor_filename for v in processed] == [
            "acme-bad-marc.dat",
            "acme-marc.dat",
        ]


def test_interface_view(
    test_airflow_client, mock_variable, mock_db, mocker  # noqa: F811
):
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        response = test_airflow_client.get('/vendor_management/interfaces/1')
        assert response.status_code == 200
        pending = response.html.find(id='pending-files')
        assert pending
        assert len(pending.find_all('tr')) == 3

        loaded = response.html.find(id='loaded-files')
        assert loaded
        assert len(loaded.find_all('tr')) == 2
        docdefs = response.html.find_all("dd", "processing_options")
        assert docdefs[0].text == "Acme ebooks package"
        assert docdefs[1].text == "666, 667"
        assert docdefs[2].text == "eb4"
        assert (
            docdefs[3].text.strip()
            == '856 subfield u contains pattern "^http:\\/\\/ebooks\\.acme\\.com.+" double_arrow subfield code: x, subfield value: eb4'
        )
        assert (
            docdefs[4].text.strip()
            == '856 (indicator1: "4", indicator2: "1") double_arrow 856 (indicator1: "4", indicator2: "0")'
        )


def test_missing_interface(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        response = test_airflow_client.get('/vendor_management/interfaces/3007')
        assert response.status_code == 404


def test_interface_edit_view(
    test_airflow_client, mock_db, mocker, mock_variable, job_profiles  # noqa: F811
):
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.job_profiles',
            return_value=[
                {"name": "Acme FTP", "id": "A8635200-F876-46E0-ACF0-8E0EFA542A3F"}
            ],
        )
        response = test_airflow_client.get('/vendor_management/interfaces/1/edit')
        assert response.status_code == 200
        note = response.html.find("textarea")
        assert note.text == "A note about Acme FTP Interface"
        templates = response.html.select("section > template")
        assert len(templates) == 3


def test_interface_edit_upload_only_view(
    test_airflow_client, mock_db, mocker, mock_variable, job_profiles  # noqa: F811
):
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.job_profiles',
            return_value=[],
        )
        response = test_airflow_client.get("/vendor_management/interfaces/2/edit")
        assert response.status_code == 200
        display_name_input = response.html.find(id="display-name")
        assert display_name_input.name == "input"
        assert display_name_input.attrs['value'] == "Acme Upload Only"


def test_reload_file(test_airflow_client, mock_db, mock_dag_run, mocker):  # noqa: F811
    mock_trigger_dag = mocker.patch(
        'libsys_airflow.plugins.vendor_app.vendor_management.trigger_dag',
        return_value=mock_dag_run,
    )
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.post('/vendor_management/files/1/load')
        assert response.status_code == 302

    with Session(mock_db()) as session:
        vendor_file = session.get(VendorFile, 1)
        assert vendor_file.status == FileStatus.loading
        assert vendor_file.dag_run_id == mock_dag_run.run_id
        assert vendor_file.expected_processing_time == mock_dag_run.execution_date

    mock_trigger_dag.assert_called_once_with(
        'default_data_processor',
        conf={
            "filename": 'acme-marc.dat',
            "vendor_uuid": '375C6E33-2468-40BD-A5F2-73F82FE56DB0',
            "vendor_interface_uuid": '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
            "dataload_profile_uuid": 'A8635200-F876-46E0-ACF0-8E0EFA542A3F',
        },
    )


def test_upload_file(
    test_airflow_client, mock_db, mock_dag_run, tmp_path, mocker  # noqa: F811
):
    mock_trigger_dag = mocker.patch(
        'libsys_airflow.plugins.vendor_app.vendor_management.trigger_dag',
        return_value=mock_dag_run,
    )
    mocker.patch(
        'libsys_airflow.plugins.vendor.paths.vendor_data_basepath',
        return_value=tmp_path,
    )
    today = date(2021, 1, 1)
    mock_date = mocker.patch('libsys_airflow.plugins.vendor.archive.date')
    mock_date.today.return_value = today

    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
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

        assert (
            tmp_path
            / 'downloads/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E/acme-extra-strength-marc.dat'
        ).exists()

        assert (
            tmp_path
            / 'archive/20210101/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E/acme-extra-strength-marc.dat'
        ).exists()

        vendor_file = session.scalars(
            select(VendorFile).where(
                VendorFile.vendor_filename == 'acme-extra-strength-marc.dat'
            )
        ).first()
        assert vendor_file
        assert vendor_file.vendor_interface_id == 1
        assert vendor_file.filesize == 35981
        assert vendor_file.status == FileStatus.loading
        assert vendor_file.archive_date == today
        assert vendor_file.dag_run_id == mock_dag_run.run_id
        assert vendor_file.expected_processing_time == mock_dag_run.execution_date

        mock_trigger_dag.assert_called_once_with(
            'default_data_processor',
            conf={
                "filename": 'acme-extra-strength-marc.dat',
                "vendor_uuid": '375C6E33-2468-40BD-A5F2-73F82FE56DB0',
                "vendor_interface_uuid": '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
                "dataload_profile_uuid": 'A8635200-F876-46E0-ACF0-8E0EFA542A3F',
            },
        )


def test_download_original_file(
    test_airflow_client, mock_db, tmp_path, mocker  # noqa: F811
):
    mocker.patch(
        'libsys_airflow.plugins.vendor.paths.vendor_data_basepath',
        return_value=tmp_path,
    )
    archive_path = (
        tmp_path
        / f"archive/{(now - timedelta(days=9)).strftime('%Y%m%d')}/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E"
    )
    archive_path.mkdir(parents=True, exist_ok=True)
    path = archive_path / "acme-marc.dat"

    shutil.copyfile("tests/vendor/0720230118.mrc", path)

    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        response = test_airflow_client.get(
            '/vendor_management/files/1/download/original'
        )
        assert response.status_code == 200
        assert response.content_type == 'application/octet-stream'
        assert response.content_length == 35981


def test_download_processed_file(
    test_airflow_client, mock_db, tmp_path, mocker  # noqa: F811
):
    mocker.patch(
        'libsys_airflow.plugins.vendor.paths.vendor_data_basepath',
        return_value=tmp_path,
    )
    downloads_path = (
        tmp_path
        / "downloads/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E"
    )

    downloads_path.mkdir(parents=True, exist_ok=True)
    path = downloads_path / "acme-marc-processed.dat"

    shutil.copyfile("tests/vendor/0720230118.mrc", path)

    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        response = test_airflow_client.get(
            '/vendor_management/files/1/download/processed'
        )
        assert response.status_code == 200
        assert response.content_type == 'application/octet-stream'
        assert response.content_length == 35981


def test_download_missing_file(
    test_airflow_client, mock_db, tmp_path, mocker  # noqa: F811
):
    mocker.patch(
        'libsys_airflow.plugins.vendor.paths.vendor_data_basepath',
        return_value=tmp_path,
    )
    downloads_path = (
        tmp_path
        / "downloads/375C6E33-2468-40BD-A5F2-73F82FE56DB0/140530EB-EE54-4302-81EE-D83B9DAC9B6E"
    )
    downloads_path.mkdir(parents=True, exist_ok=True)

    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        response = test_airflow_client.get(
            '/vendor_management/files/1/download/processed'
        )
        assert response.status_code == 302


def test_fetch(test_airflow_client, mock_db, mocker):  # noqa: F811
    mock_trigger_dag = mocker.patch(
        'libsys_airflow.plugins.vendor_app.vendor_management.trigger_dag'
    )
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.post('/vendor_management/interfaces/1/fetch')
        assert response.status_code == 302

    mock_trigger_dag.assert_called_once_with(
        'data_fetcher',
        conf={
            "vendor_interface_name": 'Acme FTP',
            "vendor_code": 'ACME',
            "vendor_uuid": '375C6E33-2468-40BD-A5F2-73F82FE56DB0',
            "vendor_interface_uuid": '140530EB-EE54-4302-81EE-D83B9DAC9B6E',
            "dataload_profile_uuid": 'A8635200-F876-46E0-ACF0-8E0EFA542A3F',
            "remote_path": "stanford/outgoing/data",
            "filename_regex": "*.mrc",
        },
    )


def test_create_upload_only(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.post(
            '/vendor_management/vendors/1/interfaces',
        )
        assert response.status_code == 302

        interface = session.scalars(
            select(VendorInterface).where(
                VendorInterface.display_name == 'Acme - Upload Only'
            )
        ).first()
        assert interface
        assert interface.vendor_id == 1
        assert interface.active
        assert interface.folio_interface_uuid is None
        assert not interface.assigned_in_folio


def test_delete_upload_only(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        # This creates the interface.
        interface = VendorInterface(
            vendor_id=1,
            display_name="Acme - Test Upload Only",
            active=True,
            assigned_in_folio=False,
        )
        session.add(interface)
        session.commit()

        # This deletes the interface.
        response = test_airflow_client.post(
            f"/vendor_management/interfaces/{interface.id}/delete",
        )
        assert response.status_code == 302

        interface = session.scalars(
            select(VendorInterface).where(
                VendorInterface.display_name == 'Acme - Test Upload Only'
            )
        ).first()
        assert not interface
