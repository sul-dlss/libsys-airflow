from datetime import date
from pathlib import Path


def vendor_data_basepath() -> Path:
    return Path("/opt/airflow/vendor-data")


def downloads_basepath() -> Path:
    return vendor_data_basepath() / "downloads"


def download_path(vendor_uuid: str, vendor_interface_uuid: str) -> Path:
    return downloads_basepath() / vendor_uuid / vendor_interface_uuid


def archive_basepath() -> Path:
    return vendor_data_basepath() / "archive"


def archive_path(vendor_uuid: str, vendor_interface_uuid: str, archive_date: date):
    return (
        archive_basepath()
        / archive_date.strftime("%Y%m%d")
        / vendor_uuid
        / vendor_interface_uuid
    )
