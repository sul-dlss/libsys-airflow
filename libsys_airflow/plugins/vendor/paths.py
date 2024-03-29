import os
from datetime import date


def vendor_data_basepath() -> str:
    return "/opt/airflow/vendor-data"


def downloads_basepath() -> str:
    return os.path.join(vendor_data_basepath(), "downloads")


def download_path(vendor_uuid: str, vendor_interface_uuid: str) -> str:
    return os.path.join(downloads_basepath(), vendor_uuid, vendor_interface_uuid)


def archive_basepath() -> str:
    return os.path.join(vendor_data_basepath(), "archive")


def archive_path(vendor_uuid: str, vendor_interface_uuid: str, archive_date: date):
    return os.path.join(
        archive_basepath(),
        archive_date.strftime("%Y%m%d"),
        vendor_uuid,
        vendor_interface_uuid,
    )
