import json

import pytest  # noqa

from libsys_airflow.plugins.google_scanning.constants import (
    STATUS_SHIPPED,
    STATUS_STAGED,
    STATUS_UNKNOWN,
)
from libsys_airflow.plugins.google_scanning.staging import (
    active_dag_runs,
    barcode_filename,
    cart_has_active_run,
    download_filename,
    is_cart_staged,
    list_shipped_carts,
    list_staged_carts,
    other_date_conflicts,
    save_staged_file,
    shipment_filestamp,
    shipped_file_path,
    staged_cart_status,
    trigger_on_campus_shipment_dag,
    trigger_stage_cart_items_dag,
    waiting_shipment_for,
)

STAGING = "libsys_airflow.plugins.google_scanning.staging"
STEM = "stanford_20260807-oncampus"


@pytest.fixture(autouse=True)
def mock_bases(tmp_path, mocker):
    mocker.patch(f"{STAGING}.GOOGLE_SCANNING_BASE", tmp_path)
    for name, dirname in (
        ("STAGED_FILES_BASE", "staged"),
        ("CARTS_WAITING_BASE", "carts-waiting"),
        ("ARCHIVED_FILES_BASE", "archived"),
        ("MARC_FILES_BASE", "marc-files"),
    ):
        mocker.patch(f"{STAGING}.{name}", tmp_path / dirname)
    mocker.patch(f"{STAGING}.timestamp", return_value="20260807T091500")
    return tmp_path


@pytest.fixture
def mock_staged_files_base(mock_bases):
    return mock_bases / "staged"


def _write_cart(cart_dir, barcodes: bytes = b"12345\n", status: dict | None = None):
    cart_dir.mkdir(parents=True)
    (cart_dir / "barcodes.txt").write_bytes(barcodes)
    if status is not None:
        (cart_dir / "status.json").write_text(json.dumps(status))
    return cart_dir


def _shipped_status(shipped_at: str = "20260807", stem: str = STEM) -> dict:
    return {
        "cart_name": "cart-1",
        "status": STATUS_SHIPPED,
        "shipped_at": shipped_at,
        "shipment_stem": stem,
    }


def test_save_staged_file_adds_timestamp(mock_staged_files_base):
    staged_path = save_staged_file("cart-1", "barcodes.txt", b"12345\n67890\n")

    assert staged_path == (
        mock_staged_files_base / "cart-1" / "barcodes_20260807T091500.txt"
    )
    assert staged_path.read_bytes() == b"12345\n67890\n"


def test_save_staged_file_replaces_existing_cart(mock_staged_files_base):
    _write_cart(mock_staged_files_base / "cart-1", status={"status": STATUS_STAGED})

    staged_path = save_staged_file("cart-1", "new.txt", b"999\n")

    assert sorted(path.name for path in staged_path.parent.iterdir()) == [
        "new_20260807T091500.txt"
    ]


def test_is_cart_staged(mock_staged_files_base):
    assert not is_cart_staged("cart-1")
    save_staged_file("cart-1", "barcodes.txt", b"12345\n")
    assert is_cart_staged("cart-1")


def test_staged_cart_status_defaults_to_unknown(mock_staged_files_base):
    assert staged_cart_status("cart-1") == {"status": STATUS_UNKNOWN}


def test_staged_cart_status_reads_status_file(mock_staged_files_base):
    status = {
        "cart_name": "cart-1",
        "staged_at": "2026-08-04T15:32:10-07:00",
        "total_barcodes": 12,
        "updated": 10,
        "missing_barcodes": ["36105130791697", "36105130791698"],
        "errors": [],
        "status": STATUS_STAGED,
        "shipped_at": None,
        "shipment_dag_run_id": None,
    }
    _write_cart(mock_staged_files_base / "cart-1", status=status)

    assert staged_cart_status("cart-1") == status


def test_staged_cart_status_handles_bad_json(mock_staged_files_base, caplog):
    cart_dir = _write_cart(mock_staged_files_base / "cart-1")
    (cart_dir / "status.json").write_text("not json")

    assert staged_cart_status("cart-1") == {"status": STATUS_UNKNOWN}
    assert "Could not parse status file" in caplog.text


def test_list_staged_carts_empty_when_missing_base(mock_staged_files_base):
    assert list_staged_carts() == []


def test_list_staged_carts(mock_staged_files_base):
    save_staged_file("cart-1", "barcodes.txt", b"12345\n")
    save_staged_file("cart-2", "barcodes.txt", b"67890\n")

    carts = list_staged_carts()

    assert len(carts) == 2
    assert {cart["cart_name"] for cart in carts} == {"cart-1", "cart-2"}
    assert all(cart["filename"] == "barcodes_20260807T091500.txt" for cart in carts)
    assert all(cart["status"] == {"status": STATUS_UNKNOWN} for cart in carts)


def test_barcode_filename_skips_status_and_shipment_copies(mock_bases):
    cart_dir = _write_cart(
        mock_bases / "archived" / "cart-1" / "20260807T210000",
        status=_shipped_status(),
    )
    (cart_dir / f"{STEM}.xml").write_text("xml")
    (cart_dir / f"{STEM}.txt").write_text("manifest")

    assert barcode_filename(cart_dir) == "barcodes.txt"


def test_barcode_filename_raises_when_missing(mock_bases):
    cart_dir = mock_bases / "staged" / "cart-1"
    cart_dir.mkdir(parents=True)
    (cart_dir / "status.json").write_text("{}")

    with pytest.raises(FileNotFoundError):
        barcode_filename(cart_dir)


def test_waiting_shipment_for(mock_bases):
    assert waiting_shipment_for("cart-1") is None
    _write_cart(mock_bases / "carts-waiting" / STEM / "cart-1")
    assert waiting_shipment_for("cart-1") == STEM


def test_shipment_filestamp():
    assert shipment_filestamp("20260810") == "stanford_20260810-oncampus"


def test_other_date_conflicts(mock_bases):
    _write_cart(mock_bases / "carts-waiting" / "stanford_20260806-oncampus" / "cart-1")
    _write_cart(mock_bases / "carts-waiting" / STEM / "cart-2")

    conflicts = other_date_conflicts(["cart-1", "cart-2", "cart-3"], "20260807")

    assert conflicts == [
        "cart-1 is waiting in the stanford_20260806-oncampus shipment; "
        "ship it with that date."
    ]


def test_list_shipped_carts_empty_when_nothing_shipped(mock_bases):
    assert list_shipped_carts() == []


def test_list_shipped_carts_waiting_cart_links_queued_files(mock_bases):
    status = _shipped_status()
    _write_cart(mock_bases / "carts-waiting" / STEM / "cart-1", status=status)
    for rel_path in (f"marc-files/new/{STEM}.xml", f"marc-files/manifests/{STEM}.txt"):
        (mock_bases / rel_path).parent.mkdir(parents=True, exist_ok=True)
        (mock_bases / rel_path).write_text("content")

    carts = list_shipped_carts()

    assert carts == [
        {
            "cart_name": "cart-1",
            "shipped_at": "20260807",
            "status": status,
            "files": [
                {
                    "path": f"carts-waiting/{STEM}/cart-1/barcodes.txt",
                    "download_filename": "barcodes.csv",
                },
                {
                    "path": f"marc-files/new/{STEM}.xml",
                    "download_filename": f"{STEM}.xml",
                },
                {
                    "path": f"marc-files/manifests/{STEM}.txt",
                    "download_filename": f"{STEM}.txt",
                },
            ],
        }
    ]


def test_list_shipped_carts_shows_latest_archived_shipment(mock_bases):
    old_stem = "stanford_20260801-oncampus"
    for ts, stem in (("20260801T210000", old_stem), ("20260807T210000", STEM)):
        archive_dir = _write_cart(
            mock_bases / "archived" / "cart-1" / ts,
            status=_shipped_status(stem[9:17], stem),
        )
        (archive_dir / f"{stem}.xml").write_text("xml")
        (archive_dir / f"{stem}.txt").write_text("manifest")
    (mock_bases / "archived" / "sal3" / "stanford_20260806-sal3").mkdir(parents=True)

    carts = list_shipped_carts()

    assert len(carts) == 1
    assert carts[0]["shipped_at"] == "20260807"
    assert [file["path"] for file in carts[0]["files"]] == [
        "archived/cart-1/20260807T210000/barcodes.txt",
        f"archived/cart-1/20260807T210000/{STEM}.xml",
        f"archived/cart-1/20260807T210000/{STEM}.txt",
    ]
    assert [file["download_filename"] for file in carts[0]["files"]] == [
        "barcodes.csv",
        f"{STEM}.xml",
        f"{STEM}.txt",
    ]


def test_list_shipped_carts_prefers_waiting_over_archived(mock_bases):
    _write_cart(
        mock_bases / "archived" / "cart-1" / "20260801T210000",
        status=_shipped_status("20260801"),
    )
    _write_cart(
        mock_bases / "carts-waiting" / STEM / "cart-1", status=_shipped_status()
    )

    carts = list_shipped_carts()

    assert len(carts) == 1
    assert carts[0]["shipped_at"] == "20260807"
    assert carts[0]["files"][0]["path"] == f"carts-waiting/{STEM}/cart-1/barcodes.txt"


def test_download_filename_renames_barcode_file_to_csv(mock_bases):
    cart_dir = _write_cart(mock_bases / "archived" / "cart-1" / "ts")
    assert download_filename(cart_dir / "barcodes.txt") == "barcodes.csv"


def test_download_filename_keeps_manifest_name(mock_bases):
    cart_dir = _write_cart(
        mock_bases / "archived" / "cart-1" / "ts", status=_shipped_status()
    )
    assert download_filename(cart_dir / f"{STEM}.txt") == f"{STEM}.txt"


def test_shipped_file_path(mock_bases):
    _write_cart(mock_bases / "archived" / "cart-1" / "ts")

    file_path = shipped_file_path("archived/cart-1/ts/barcodes.txt")

    assert file_path == (mock_bases / "archived/cart-1/ts/barcodes.txt").resolve()


@pytest.mark.parametrize(
    "rel_path",
    [
        "../../etc/passwd",
        "archived/../../etc/passwd",
        "staged/cart-1/barcodes.txt",
    ],
)
def test_shipped_file_path_rejects_paths_outside_shipped_dirs(mock_bases, rel_path):
    with pytest.raises(ValueError):
        shipped_file_path(rel_path)


def test_cart_has_active_run(mocker):
    mocker.patch(
        f"{STAGING}.active_dag_runs",
        return_value=[{"progress_keys": ["cart-1"]}],
    )

    assert cart_has_active_run("cart-1")
    assert not cart_has_active_run("cart-2")


def test_trigger_stage_cart_items_dag(mocker):
    mock_api_client = mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.api_client"
    )
    mock_dag_run_api = mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.DagRunApi"
    )
    mock_api_instance = mock_dag_run_api.return_value
    mock_api_instance.trigger_dag_run.return_value.dag_run_id = "run-123"

    dag_run_id = trigger_stage_cart_items_dag("/path/to/barcodes.txt", "cart-1")

    assert dag_run_id == "run-123"
    mock_api_client.return_value.__enter__.assert_called_once()
    mock_api_instance.trigger_dag_run.assert_called_once()
    call_args = mock_api_instance.trigger_dag_run.call_args
    assert call_args[0][0] == "stage_cart_items"
    assert call_args[0][1].conf == {
        "staged_file_path": "/path/to/barcodes.txt",
        "cart_name": "cart-1",
    }


def test_trigger_on_campus_shipment_dag(mocker):
    mocker.patch("libsys_airflow.plugins.google_scanning.staging.api_client")
    mock_dag_run_api = mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.DagRunApi"
    )
    mock_api_instance = mock_dag_run_api.return_value
    mock_api_instance.trigger_dag_run.return_value.dag_run_id = "run-456"

    selected_carts = [{"cart_name": "cart-1", "filename": "barcodes.txt"}]
    dag_run_id = trigger_on_campus_shipment_dag(
        selected_carts, "staff@example.com", "2026-08-07"
    )

    assert dag_run_id == "run-456"
    call_args = mock_api_instance.trigger_dag_run.call_args
    assert call_args[0][0] == "on_campus_shipment"
    assert call_args[0][1].conf == {
        "selected_carts": selected_carts,
        "user_email": "staff@example.com",
        "shipped_at": "20260807",
    }


def test_active_dag_runs(mocker):
    mock_shared_active_dag_runs = mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.shared_active_dag_runs",
        return_value=[
            {
                "dag_id": "stage_cart_items",
                "dag_run_id": "stage-run",
                "state": "running",
                "conf": {"cart_name": "cart-1"},
                "finished_tasks": 1,
                "total_tasks": 4,
                "running_tasks": ["process_barcodes_batch"],
            },
            {
                "dag_id": "on_campus_shipment",
                "dag_run_id": "ship-run",
                "state": "queued",
                "conf": {
                    "selected_carts": [
                        {"cart_name": "cart-2", "filename": "a.txt"},
                        {"cart_name": "cart-3", "filename": "b.txt"},
                    ]
                },
                "finished_tasks": 0,
                "total_tasks": 0,
                "running_tasks": [],
            },
        ],
    )

    runs = active_dag_runs()

    mock_shared_active_dag_runs.assert_called_once_with(
        ("stage_cart_items", "on_campus_shipment")
    )
    assert runs == [
        {
            "dag_id": "stage_cart_items",
            "dag_run_id": "stage-run",
            "state": "running",
            "progress_keys": ["cart-1"],
            "finished_tasks": 1,
            "total_tasks": 4,
            "running_tasks": ["process_barcodes_batch"],
        },
        {
            "dag_id": "on_campus_shipment",
            "dag_run_id": "ship-run",
            "state": "queued",
            "progress_keys": ["cart-2", "cart-3"],
            "finished_tasks": 0,
            "total_tasks": 0,
            "running_tasks": [],
        },
    ]


def test_active_dag_runs_none_active(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.shared_active_dag_runs",
        return_value=[],
    )

    assert active_dag_runs() == []
