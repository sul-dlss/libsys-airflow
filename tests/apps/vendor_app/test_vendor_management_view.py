from datetime import datetime, timedelta

import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from airflow.models import Variable
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import Vendor, VendorInterface, VendorFile
from tests.airflow_client import test_airflow_client  # noqa: F401


now = datetime.utcnow()

rows = Rows(
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="375C6E33-2468-40BD-A5F2-73F82FE56DB0",
        vendor_code_from_folio="ACME",
        acquisitions_unit_from_folio="ACMEUNIT",
        last_folio_update=datetime.utcnow(),
    ),
    Vendor(
        id=2,
        display_name="Cocina Tacos",
        folio_organization_uuid="42E8DECC-6DE4-48C1-8F04-8578FF1BEA71",
        vendor_code_from_folio="COCINA",
        acquisitions_unit_from_folio="COCINAUNIT",
        last_folio_update=datetime.utcnow(),
    ),
    VendorInterface(
        id=1,
        display_name="Acme FTP",
        vendor_id=1,
        folio_interface_uuid="140530EB-EE54-4302-81EE-D83B9DAC9B6E",
        folio_data_import_processing_name="Acme Profile 1",
        file_pattern="*.mrc",
        remote_path="stanford/outgoing/data",
        processing_dag="acme-pull",
        processing_delay_in_days=3,
        active=False,
    ),
    VendorInterface(
        id=2,
        display_name="Acme API",
        vendor_id=1,
        folio_interface_uuid="A8635200-F876-46E0-ACF0-8E0EFA542A3F",
        folio_data_import_processing_name="Acme Profile 2",
        file_pattern="*.mrc",
        remote_path="stanford/outgoing/data",
        processing_dag="acme-pull",
        processing_delay_in_days=3,
        active=False,
    ),
    VendorFile(
        id=1,
        vendor_interface_id=1,
        vendor_filename="records123.mrc",
        filesize="3049",
        status="fetching_error",
        created=now - timedelta(days=2),
        updated=now - timedelta(days=1),
        vendor_timestamp=now - timedelta(days=14),
    ),
    VendorFile(
        id=2,
        vendor_interface_id=1,
        vendor_filename="records234.mrc",
        filesize="1014",
        status="loading_error",
        created=now - timedelta(days=1),
        updated=now,
        vendor_timestamp=now - timedelta(days=14),
    ),
    VendorFile(
        id=3,
        vendor_interface_id=1,
        vendor_filename="records345.mrc",
        filesize="2000",
        status="loading",
        created=now - timedelta(days=1),
        updated=now,
        vendor_timestamp=now - timedelta(days=14),
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def mock_okapi_url_variable(monkeypatch):
    def mock_get(key):
        return "https://okapi-test.stanford.edu"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    yield mock_hook


def test_vendors_dashboard_view(
    test_airflow_client, mock_db, mocker, mock_okapi_url_variable  # noqa: F811
):
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.get('/vendor_management/')
        assert response.status_code == 200
        assert response.html.h1.text == "Vendor Management"
        error_table = response.html.find(id='errorsTable')
        error_rows = error_table.find_all('tr')
        assert len(error_rows) == 2
        retry_cell = error_rows[0].find_all('td')[-1]
        assert retry_cell.find(class_='btn')["value"] == "Retry"
        assert retry_cell.form["action"].startswith(
            "/vendor_management/files/1/reset_fetch"
        )
        retry_cell2 = error_rows[1].find_all('td')[-1]
        assert retry_cell2.find(class_='btn')["value"] == "Retry"
        assert retry_cell2.form["action"].startswith("/vendor_management/files/2/load")


def test_vendors_index_view(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.get('/vendor_management/vendors')
        assert response.status_code == 200
        assert response.html.h1.text == "Vendors"

        rows = response.html.find_all('tr')
        assert len(rows) == 2

        link = rows[0].find_all('td')[0].a
        assert link.text == "Acme"
        assert link["href"] == "/vendor_management/vendors/1"

        link = rows[1].find_all('td')[0].a
        assert link.text == "Cocina Tacos"
        assert link["href"] == "/vendor_management/vendors/2"


def test_vendor_show_view(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.get('/vendor_management/vendors/1')
        assert response.status_code == 200
        assert response.html.h1.text == "Acme"

        rows = response.html.find_all('tr')
        assert len(rows) == 2

        assert 'Acme FTP' in rows[0].text
        assert 'Acme API' in rows[1].text


def test_missing_vendor(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.get('/vendor_management/vendors/987')
        assert response.status_code == 404
