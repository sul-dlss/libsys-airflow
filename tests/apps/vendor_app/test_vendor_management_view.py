from datetime import datetime

import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import Vendor, VendorInterface
from tests.airflow_client import test_airflow_client  # noqa: F401


rows = Rows(
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="375C6E33-2468-40BD-A5F2-73F82FE56DB0",
        vendor_code_from_folio="ACME",
        acquisitions_unit_from_folio="ACMEUNIT",
        has_active_vendor_interfaces=False,
        last_folio_update=datetime.now(),
    ),
    Vendor(
        id=2,
        display_name="Cocina Tacos",
        folio_organization_uuid="42E8DECC-6DE4-48C1-8F04-8578FF1BEA71",
        vendor_code_from_folio="COCINA",
        acquisitions_unit_from_folio="COCINAUNIT",
        has_active_vendor_interfaces=False,
        last_folio_update=datetime.now(),
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
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    yield mock_hook


def test_vendors_dashboard_view(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
        )
        response = test_airflow_client.get('/vendor_management/')
        assert response.status_code == 200
        assert response.html.h1.text == "Vendor Management"


def test_vendors_index_view(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
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
            'libsys_airflow.plugins.vendor_app.vendors.Session', return_value=session
        )
        response = test_airflow_client.get('/vendor_management/vendors/1')
        assert response.status_code == 200
        assert response.html.h1.text == "Acme"

        rows = response.html.find_all('tr')
        assert len(rows) == 2

        assert 'Acme FTP' in rows[0].text
        assert 'Acme API' in rows[1].text
