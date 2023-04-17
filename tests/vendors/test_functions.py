import pytest

from libsys_airflow.plugins.vendors import (
    backup_retrieved_files,
    check_retrieve_files,
    rename_vendor_files,
    zip_extraction
)

def test_backup_retrieved_files(caplog):
    assert backup_retrieved_files

def test_check_retrieve_files():
    assert check_retrieve_files

def test_rename_vendor_files():
    assert rename_vendor_files

def test_zip_extraction():
    assert zip_extraction