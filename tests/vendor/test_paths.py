import pathlib
import pytest  # noqa
from datetime import datetime

from libsys_airflow.plugins.vendor.paths import (
    vendor_data_basepath,
    downloads_basepath,
    download_path,
    archive_basepath,
    archive_path,
)


@pytest.fixture
def organization_uuid():
    return "9cce436e-1858-4c37-9c7f-9374a36576ff"


@pytest.fixture
def vendor_interface_uuid():
    return "65d30c15-a560-4064-be92-f90e38eeb351"


def test_vendor_data_basepath():
    assert vendor_data_basepath() == pathlib.Path("/opt/airflow/vendor-data")


def test_downloads_basepath():
    assert downloads_basepath() == pathlib.Path("/opt/airflow/vendor-data/downloads")


def test_download_path(organization_uuid, vendor_interface_uuid):
    assert download_path(organization_uuid, vendor_interface_uuid) == pathlib.Path(
        "/opt/airflow/vendor-data/downloads/9cce436e-1858-4c37-9c7f-9374a36576ff/65d30c15-a560-4064-be92-f90e38eeb351"
    )


def test_archive_basepath():
    assert archive_basepath() == pathlib.Path("/opt/airflow/vendor-data/archive")


def test_archive_path(organization_uuid, vendor_interface_uuid):
    assert archive_path(
        organization_uuid, vendor_interface_uuid, datetime(2023, 1, 1)
    ) == pathlib.Path(
        "/opt/airflow/vendor-data/archive/20230101/9cce436e-1858-4c37-9c7f-9374a36576ff/65d30c15-a560-4064-be92-f90e38eeb351"
    )
