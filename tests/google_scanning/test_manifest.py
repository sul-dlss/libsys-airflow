import pathlib

import pytest  # noqa

from libsys_airflow.plugins.google_scanning.manifest import (
    generate_manifest,
    manifest_filename,
)


@pytest.fixture(autouse=True)
def mock_marc_files_base(tmp_path, mocker):
    base = tmp_path / "marc-files"
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.manifest.MARC_FILES_BASE", base
    )
    return base


def test_manifest_filename():
    assert (
        manifest_filename("stanford_20260810-oncampus")
        == "stanford_20260810-oncampus.txt"
    )


def test_generate_manifest_writes_cart_then_barcode_in_given_order(
    mock_marc_files_base,
):
    pairs = [("222", "cart-2"), ("333", "cart-1"), ("111", "cart-1")]

    manifest_path = generate_manifest(pairs, "stanford_20260810-oncampus")

    assert manifest_path == str(
        mock_marc_files_base / "manifests" / "stanford_20260810-oncampus.txt"
    )
    content = pathlib.Path(manifest_path).read_text()
    assert content == "cart-2\t222\ncart-1\t333\ncart-1\t111\n"


def test_generate_manifest_creates_manifests_directory(mock_marc_files_base):
    generate_manifest([("111", "cart-1")], "stamp")

    assert (mock_marc_files_base / "manifests").is_dir()


def test_generate_manifest_empty_pairs(mock_marc_files_base):
    manifest_path = generate_manifest([], "stamp")

    assert pathlib.Path(manifest_path).read_text() == ""
