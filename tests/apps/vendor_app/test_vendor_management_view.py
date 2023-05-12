from datetime import datetime

import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows

from libsys_airflow.plugins.vendor.models import Vendor
from tests.client import test_airflow_client


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
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    yield mock_hook


def test_vendor_views(test_airflow_client, mock_db):
    response = test_airflow_client.get('/vendors/')
    assert response.status_code == 200
    assert "Vendors" in response.text
    assert "ACME" in response.text
    assert "Cocina Tacos" in response.text
