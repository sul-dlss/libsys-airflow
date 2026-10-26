import json
import pathlib

import pytest

from libsys_airflow.plugins.google_scanning.transmission import (
    archive_sent_shipments,
    sendable_shipment_files,
)

CAMPUS_STEM = "stanford_20260810-oncampus"
SAL3_STEM = "stanford_20260809-sal3"


@pytest.fixture(autouse=True)
def mock_bases(tmp_path, mocker):
    for module in ("transmission", "shipment"):
        mocker.patch(
            f"libsys_airflow.plugins.google_scanning.{module}.CARTS_WAITING_BASE",
            tmp_path / "carts-waiting",
        )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.transmission.ARCHIVED_FILES_BASE",
        tmp_path / "archived",
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.transmission.timestamp",
        return_value="20260810T210000",
    )
    return tmp_path


def _shipment_files(base, stem: str) -> list[str]:
    files = [
        base / "marc-files" / "new" / f"{stem}.xml",
        base / "marc-files" / "manifests" / f"{stem}.txt",
    ]
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(f"{file.suffix} for {stem}")
    return [str(file) for file in files]


def _waiting_shipment(base, stem: str, carts: list[str], status: str = "ready"):
    shipment_dir = base / "carts-waiting" / stem
    for cart_name in carts:
        cart_dir = shipment_dir / cart_name
        cart_dir.mkdir(parents=True)
        (cart_dir / "barcodes.txt").write_text(f"{cart_name}\n")
        (cart_dir / "status.json").write_text(json.dumps({"status": "shipped"}))
    (shipment_dir / "shipment.json").write_text(
        json.dumps({"stem": stem, "carts": carts, "status": status})
    )


def test_sendable_shipment_files_keeps_ready_pairs(mock_bases):
    campus_files = _shipment_files(mock_bases, CAMPUS_STEM)
    sal3_files = _shipment_files(mock_bases, SAL3_STEM)
    _waiting_shipment(mock_bases, CAMPUS_STEM, ["cart-1"])

    result = sendable_shipment_files(campus_files + sal3_files)

    assert sorted(result["file_list"]) == sorted(campus_files + sal3_files)
    assert result["skipped"] == []


def test_sendable_shipment_files_skips_incomplete_and_building(mock_bases):
    building_files = _shipment_files(mock_bases, CAMPUS_STEM)
    _waiting_shipment(mock_bases, CAMPUS_STEM, ["cart-1"], status="building")
    xml_only = _shipment_files(mock_bases, "stanford_20260801-oncampus")[:1]
    manifest_only = _shipment_files(mock_bases, SAL3_STEM)[1:]

    result = sendable_shipment_files(building_files + xml_only + manifest_only)

    assert result["file_list"] == []
    assert {skip["stem"]: skip["reason"] for skip in result["skipped"]} == {
        "stanford_20260801-oncampus": "missing its manifest file",
        CAMPUS_STEM: "still being built, or its last build failed; "
        "trigger the shipment again to send it",
        SAL3_STEM: "missing its MARCXML file",
    }


def test_archive_sent_shipments_archives_each_campus_cart(mock_bases):
    files = _shipment_files(mock_bases, CAMPUS_STEM)
    _waiting_shipment(mock_bases, CAMPUS_STEM, ["cart-1", "cart-2"])

    archived = archive_sent_shipments(files)

    archived_base = mock_bases / "archived"
    assert archived == [
        str(archived_base / "cart-1" / "20260810T210000"),
        str(archived_base / "cart-2" / "20260810T210000"),
    ]
    for cart_name in ("cart-1", "cart-2"):
        archive_dir = archived_base / cart_name / "20260810T210000"
        assert sorted(path.name for path in archive_dir.iterdir()) == [
            "barcodes.txt",
            f"{CAMPUS_STEM}.txt",
            f"{CAMPUS_STEM}.xml",
            "status.json",
        ]
    assert not (mock_bases / "carts-waiting" / CAMPUS_STEM).exists()
    # The originals stay for move_transmitted_data_task
    assert all(pathlib.Path(file).exists() for file in files)


def test_archive_sent_shipments_keeps_earlier_archives(mock_bases):
    earlier = mock_bases / "archived" / "cart-1" / "20260801T210000"
    earlier.mkdir(parents=True)
    files = _shipment_files(mock_bases, CAMPUS_STEM)
    _waiting_shipment(mock_bases, CAMPUS_STEM, ["cart-1"])

    archive_sent_shipments(files)

    assert sorted(path.name for path in earlier.parent.iterdir()) == [
        "20260801T210000",
        "20260810T210000",
    ]


def test_archive_sent_shipments_copies_sal3_shipment(mock_bases):
    files = _shipment_files(mock_bases, SAL3_STEM)

    archived = archive_sent_shipments(files)

    archive_dir = mock_bases / "archived" / "sal3" / SAL3_STEM
    assert archived == [str(archive_dir)]
    assert sorted(path.name for path in archive_dir.iterdir()) == [
        f"{SAL3_STEM}.txt",
        f"{SAL3_STEM}.xml",
    ]
