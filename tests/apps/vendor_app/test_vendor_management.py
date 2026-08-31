import pytest
from datetime import datetime, timezone

from csrf_helpers import csrf_test_client  # noqa
from pytest_mock_resources import Rows
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from libsys_airflow.plugins.vendor.models import (
    Model,
    Vendor,
    VendorInterface,
)
from libsys_airflow.plugins.vendor_app.vendor_management import app

now = datetime.now(timezone.utc)

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
        processing_dag=None,
        processing_options=None,
        processing_delay_in_days=None,
        active=True,
        assigned_in_folio=True,
        additional_email_recipients=None,
    ),
)

client = csrf_test_client(app, follow_redirects=False)


@pytest.fixture
def engine():
    # FastAPI's TestClient dispatches requests on a background thread, so the
    # sqlite connection must be shared (StaticPool) and thread-unsafe checks
    # disabled, otherwise the view's DB session can't see the seeded rows.
    test_engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Model.metadata.create_all(test_engine)
    with Session(test_engine) as session:
        rows.apply(session)
    return test_engine


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


def test_update_vendor_interface_form(mock_db, mocker):
    with Session(mock_db()) as session:
        mocker.patch(
            "libsys_airflow.plugins.vendor_app.vendor_management.Session",
            return_value=session,
        )
        response = client.post(
            "/interfaces/1/edit",
            data={
                "package-name": "Acme ebooks package",
                "prepend-001": "eb4",
                "remove-field-1": "666",
                "remove-field-2": "667",
                "add-subfield-tag-1": "856",
                "add-subfield-eval-1": "u",
                "add-subfield-pattern-1": "^http:\\/\\/ebooks\\.acme\\.com.+",
                "add-subfield-code-1": "x",
                "add-subfield-value-1": "eb4",
                "move-field-from-1": "655",
                "move-indicator1-from-1": "",
                "move-indicator2-from-1": "",
                "move-field-to-1": "650",
                "move-indicator1-to-1": "",
                "move-indicator2-to-1": "7",
            },
        )
        assert response.status_code == 302

        interface = session.query(VendorInterface).get(1)
        assert interface.processing_options["archive_regex"] == ""
        assert interface.processing_options["package_name"] == "Acme ebooks package"
        assert interface.processing_options["prepend_001"]["data"] == "eb4"
        assert interface.processing_options["delete_marc"] == ["666", "667"]
        assert interface.processing_options["add_subfield"][0]["tag"] == "856"
        assert interface.processing_options["add_subfield"][0]["eval_subfield"] == "u"
        assert (
            interface.processing_options["add_subfield"][0]["pattern"]
            == "^http:\\/\\/ebooks\\.acme\\.com.+"
        )
        assert (
            interface.processing_options["add_subfield"][0]["subfields"][0]["code"]
            == "x"
        )
        assert (
            interface.processing_options["add_subfield"][0]["subfields"][0]["value"]
            == "eb4"
        )
        assert interface.processing_options["change_marc"][0]["from"]["tag"] == "655"
        assert (
            interface.processing_options["change_marc"][0]["from"]["indicator1"] == ""
        )
        assert (
            interface.processing_options["change_marc"][0]["from"]["indicator2"] == ""
        )
        assert interface.processing_options["change_marc"][0]["to"]["tag"] == "650"
        assert interface.processing_options["change_marc"][0]["to"]["indicator1"] == ""
        assert interface.processing_options["change_marc"][0]["to"]["indicator2"] == "7"


def test_update_vendor_interface_empty_form_values(mock_db, mocker):
    with Session(mock_db()) as session:
        mocker.patch(
            "libsys_airflow.plugins.vendor_app.vendor_management.Session",
            return_value=session,
        )
        response = client.post(
            "/interfaces/1/edit",
            data={
                "package-name": "",
                "prepend-001": "eb4",
                "remove-field-1": "666",
                "remove-field-2": "667",
                "add-subfield-tag-1": "856",
                "add-subfield-eval-1": "",
                "add-subfield-pattern-1": "",
                "add-subfield-code-1": "x",
                "add-subfield-value-1": "eb4",
            },
        )
        assert response.status_code == 302
        interface = session.query(VendorInterface).get(1)
        assert interface.processing_options["package_name"] == ""
        assert interface.processing_options["prepend_001"]["data"] == "eb4"
        assert interface.processing_options["delete_marc"] == ["666", "667"]
        assert interface.processing_options["add_subfield"][0]["tag"] == "856"
        assert interface.processing_options["add_subfield"][0]["eval_subfield"] == ""
        assert interface.processing_options["add_subfield"][0]["pattern"] == ""
        assert (
            interface.processing_options["add_subfield"][0]["subfields"][0]["code"]
            == "x"
        )
        assert (
            interface.processing_options["add_subfield"][0]["subfields"][0]["value"]
            == "eb4"
        )
