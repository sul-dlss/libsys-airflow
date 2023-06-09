import pytest  # noqa
from pytest_mock_resources import create_sqlite_fixture, Rows
from unittest.mock import MagicMock
from datetime import datetime

from sqlalchemy.orm import Session

from airflow.providers.postgres.hooks.postgres import PostgresHook
from libsys_airflow.plugins.vendor.sync import sync_data
from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
)


rows = Rows(
    Vendor(
        id=1,
        display_name='Acme',
        folio_organization_uuid='698a62fe-8aff-40c7-b1ef-e8bd13c77536',
        vendor_code_from_folio='ACME',
        acquisitions_unit_from_folio='aa000c65-1234-41f7-8361-1c1e8433333',
        acquisitions_unit_name_from_folio='STANFORD_UNIT',
        last_folio_update=datetime.now(),
    ),
    Vendor(
        id=2,
        display_name='Gobi',
        folio_organization_uuid='123b45cd-8aff-40c7-b1ef-e8bd13c11111',
        vendor_code_from_folio='GOBI-SUL',
        acquisitions_unit_from_folio='aa000c65-1234-41f7-8361-1c1e8433333',
        acquisitions_unit_name_from_folio='STANFORD_UNIT',
        last_folio_update=datetime.now(),
    ),
    VendorInterface(
        id=1,
        display_name='OCLC - Full bibs',
        vendor_id=1,
        folio_interface_uuid='65d30c15-a560-4064-be92-f90e38eeb351',
        folio_data_import_profile_uuid='f4144dbd-def7-4b77-842a-954c62faf319',
        active=True,
        assigned_in_folio=True,
    ),
    VendorInterface(
        id=2,
        display_name='Test Interface',
        vendor_id=2,
        folio_interface_uuid='111z22zz-a000-4064-be92-c22e38eeb000',
        folio_data_import_profile_uuid='f4144dbd-def7-4b77-842a-954c62faf319',
        active=True,
        assigned_in_folio=True,
    ),
    VendorInterface(
        id=3,
        display_name='Records Service',
        vendor_id=2,
        folio_interface_uuid='45678d90-b560-4064-be92-f90e38aaa222',
        folio_data_import_profile_uuid='f4144dbd-def7-4b77-842a-954c62faf319',
        active=True,
        assigned_in_folio=True,
    ),
    VendorInterface(
        id=4,
        display_name='Records Service',
        vendor_id=1,
        folio_interface_uuid='45678d90-b560-4064-be92-f90e38aaa222',
        folio_data_import_profile_uuid='f4144dbd-def7-4b77-842a-954c62faf319',
        active=False,
        assigned_in_folio=False,
    ),
    VendorInterface(
        id=5,
        display_name='Upload-only interface',
        vendor_id=1,
        folio_interface_uuid=None,
        folio_data_import_profile_uuid='f4144dbd-def7-4b77-842a-954c62faf319',
        active=True,
        assigned_in_folio=False,
    ),
)

engine = create_sqlite_fixture(rows)

organizations_resp = [
    {
        'id': '698a62fe-8aff-40c7-b1ef-e8bd13c77536',
        'name': 'Acme',
        'code': 'ACME',
        'status': 'Active',
        'interfaces': [
            '65d30c15-a560-4064-be92-f90e38eeb351',
            '45678d90-b560-4064-be92-f90e38aaa222',
        ],
        'isVendor': True,
        'acqUnitIds': ['aa000c65-1234-41f7-8361-1c1e8433333'],
    },
    {
        'id': '123b45cd-8aff-40c7-b1ef-e8bd13c11111',
        'name': 'Gobi New Name',
        'code': 'GOBI-SUL',
        'status': 'Active',
        'interfaces': ['111z22zz-a000-4064-be92-c22e38eeb000'],
        'isVendor': True,
        'acqUnitIds': ['aa000c65-1234-41f7-8361-1c1e8433333'],
    },
    {
        'id': '97865432-w8af-40c7-1234-e8888888888',
        'name': 'New Vendor',
        'code': 'NEWVEN',
        'status': 'Active',
        'interfaces': ['45678d90-b560-4064-be92-f90e38aaa222'],
        'isVendor': True,
        'acqUnitIds': ['aa000c65-1234-41f7-8361-1c1e8433333'],
    },
]
organization_resp = {
    'id': '123b45cd-8aff-40c7-b1ef-e8bd13c11111',
    'name': 'Gobi New Name',
    'code': 'GOBI-SUL',
    'status': 'Active',
    'interfaces': ['111z22zz-a000-4064-be92-c22e38eeb000'],
    'isVendor': True,
    'acqUnitIds': ['aa000c65-1234-41f7-8361-1c1e8433333'],
}
interface_resp_1 = {
    'id': '65d30c15-a560-4064-be92-f90e38eeb351',
    'name': 'OCLC - Full bibs',
}
interface_resp_2 = {
    'id': '111z22zz-a000-4064-be92-c22e38eeb000',
    'name': 'Test Interface',
}
interface_resp_3 = {
    'id': '45678d90-b560-4064-be92-f90e38aaa222',
    'name': 'Records Service',
}
acq_unit_resp = 'NEW-STANFORD-UNIT'


@pytest.fixture
def folio_org_uuid():
    return '123b45cd-8aff-40c7-b1ef-e8bd13c11111'


@pytest.fixture
def mock_folio_client():
    mock_client = MagicMock()
    mock_client.organizations = organizations_resp
    mock_client.folio_get.side_effect = mock_folio_get
    return mock_client


def mock_folio_get(*args, **kwargs):
    if args[0].endswith('aa000c65-1234-41f7-8361-1c1e8433333'):
        return acq_unit_resp
    elif args[0].endswith('65d30c15-a560-4064-be92-f90e38eeb351'):
        return interface_resp_1
    elif args[0].endswith('111z22zz-a000-4064-be92-c22e38eeb000'):
        return interface_resp_2
    elif args[0].endswith('45678d90-b560-4064-be92-f90e38aaa222'):
        return interface_resp_3
    elif args[0].endswith('123b45cd-8aff-40c7-b1ef-e8bd13c11111'):
        return organization_resp
    else:
        return 'Response not found'


@pytest.fixture
def pg_hook(mocker, engine) -> PostgresHook:
    mock_hook = mocker.patch(
        'airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine'
    )
    mock_hook.return_value = engine
    return mock_hook


def test_sync(mock_folio_client, pg_hook):
    folio_org_id = None
    sync_data(folio_org_id, mock_folio_client)

    with Session(pg_hook()) as session:
        vendor_1 = session.query(Vendor).get(1)
        vendor_2 = session.query(Vendor).get(2)
        assert vendor_1.display_name == 'Acme'
        # vendor display name updated from FOLIO
        assert vendor_2.display_name == 'Gobi New Name'
        # acquisitions unit updated from FOLIO
        assert vendor_2.acquisitions_unit_name_from_folio == 'NEW-STANFORD-UNIT'
        assert (
            vendor_2.acquisitions_unit_from_folio
            == 'aa000c65-1234-41f7-8361-1c1e8433333'
        )

        interface_3 = session.query(VendorInterface).get(3)
        # interface was unassigned from Vendor 2 in FOLIO and is no longer active
        assert interface_3.assigned_in_folio is False
        assert interface_3.active is False

        restored_interface = session.query(VendorInterface).get(4)
        # interface that had been unassigned is assigned to vendor again and starts as inactive
        assert (
            restored_interface.folio_interface_uuid
            == '45678d90-b560-4064-be92-f90e38aaa222'
        )
        assert restored_interface.assigned_in_folio is True
        assert restored_interface.active is False

        upload_interface = session.query(VendorInterface).get(5)
        # upload-only interface is not changed in sync
        assert upload_interface.active is True
        assert upload_interface.assigned_in_folio is False

        new_vendor = (
            session.query(Vendor)
            .filter(
                Vendor.folio_organization_uuid == '97865432-w8af-40c7-1234-e8888888888'
            )
            .first()
        )
        new_interface = (
            session.query(VendorInterface)
            .filter(VendorInterface.vendor_id == 3)
            .first()
        )
        # new vendor and vendor interface added
        assert new_vendor.id == 3
        assert new_vendor.vendor_code_from_folio == 'NEWVEN'
        assert new_interface.id == 6
        assert (
            new_interface.folio_interface_uuid == '45678d90-b560-4064-be92-f90e38aaa222'
        )


def test_sync_on_demand(folio_org_uuid, mock_folio_client, pg_hook):
    sync_data(folio_org_uuid, mock_folio_client)

    with Session(pg_hook()) as session:
        vendor = session.query(Vendor).get(2)
        # vendor display name updated from FOLIO
        assert vendor.display_name == 'Gobi New Name'
        # acquisitions unit updated from FOLIO
        assert vendor.acquisitions_unit_name_from_folio == 'NEW-STANFORD-UNIT'
        assert (
            vendor.acquisitions_unit_from_folio == 'aa000c65-1234-41f7-8361-1c1e8433333'
        )
