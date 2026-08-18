import re

import pytest

from libsys_airflow.plugins.google_scanning.marc import (
    generate_marc_for_instances,
    generate_shipment_marc,
    shipment_filestamp,
)


def test_shipment_filestamp_format():
    filestamp = shipment_filestamp("20260810")

    assert re.match(r"^stanford_20260810-campus-\d{6}$", filestamp)


def test_generate_shipment_marc_success(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.shipment_filestamp",
        return_value="stanford_20260810-campus-143022",
    )
    mock_save_ids = mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.save_ids",
        return_value=(
            "/opt/airflow/data-export-files/google_scanning/instanceids/new/"
            "stanford_20260810-campus-143022.csv"
        ),
    )
    mock_marc_for_instances = mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.marc_for_instances",
        return_value={
            "new": [
                "/opt/airflow/data-export-files/google_scanning/marc-files/new/"
                "stanford_20260810-campus-143022.mrc"
            ],
            "updates": [],
            "deletes": [],
            "not_found": ["instance-2"],
        },
    )
    mock_add_holdings = mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.add_holdings_items_to_marc_files"
    )
    mock_clean_serialize = mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.clean_and_serialize_marc_files"
    )

    result = generate_shipment_marc(["instance-1", "instance-2"], "20260810")

    mock_save_ids.assert_called_once_with(
        vendor="google_scanning",
        kind="new",
        data=["instance-1", "instance-2"],
        timestamp="stanford_20260810-campus-143022",
    )
    mock_marc_for_instances.assert_called_once_with(
        instance_files=[mock_save_ids.return_value]
    )
    mock_add_holdings.assert_called_once_with(
        mock_marc_for_instances.return_value, full_dump=False
    )
    mock_clean_serialize.assert_called_once_with(mock_marc_for_instances.return_value)

    assert result == {
        "filestamp": "stanford_20260810-campus-143022",
        "marc_xml_path": (
            "/opt/airflow/data-export-files/google_scanning/marc-files/new/"
            "stanford_20260810-campus-143022.xml"
        ),
        "not_found_instance_ids": ["instance-2"],
    }


def test_generate_shipment_marc_dedupes_instance_ids(mocker):
    mock_save_ids = mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.save_ids",
        return_value="ids.csv",
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.marc_for_instances",
        return_value={"new": ["file.mrc"], "not_found": []},
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.add_holdings_items_to_marc_files"
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.clean_and_serialize_marc_files"
    )

    generate_shipment_marc(["instance-1", "instance-1", "instance-2"], "20260810")

    assert mock_save_ids.call_args[1]["data"] == ["instance-1", "instance-2"]


def test_generate_shipment_marc_raises_when_save_ids_returns_none(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.save_ids", return_value=None
    )

    with pytest.raises(ValueError, match="No instance ids to generate MARC for"):
        generate_shipment_marc([], "20260810")


def test_generate_shipment_marc_raises_when_no_marc_generated(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.save_ids",
        return_value="ids.csv",
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.marc_for_instances",
        return_value={"new": [], "not_found": ["instance-1"]},
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.add_holdings_items_to_marc_files"
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.clean_and_serialize_marc_files"
    )

    with pytest.raises(ValueError, match="No MARC records generated"):
        generate_shipment_marc(["instance-1"], "20260810")


def test_generate_marc_for_instances_uses_given_filestamp(mocker):
    mock_save_ids = mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.save_ids",
        return_value=(
            "/opt/airflow/data-export-files/google_scanning/instanceids/new/"
            "stanford_20260813-sal3.csv"
        ),
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.marc_for_instances",
        return_value={
            "new": [
                "/opt/airflow/data-export-files/google_scanning/marc-files/new/"
                "stanford_20260813-sal3.mrc"
            ],
            "not_found": [],
        },
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.add_holdings_items_to_marc_files"
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.marc.clean_and_serialize_marc_files"
    )

    result = generate_marc_for_instances(["instance-1"], "stanford_20260813-sal3")

    mock_save_ids.assert_called_once_with(
        vendor="google_scanning",
        kind="new",
        data=["instance-1"],
        timestamp="stanford_20260813-sal3",
    )
    assert result == {
        "filestamp": "stanford_20260813-sal3",
        "marc_xml_path": (
            "/opt/airflow/data-export-files/google_scanning/marc-files/new/"
            "stanford_20260813-sal3.xml"
        ),
        "not_found_instance_ids": [],
    }
