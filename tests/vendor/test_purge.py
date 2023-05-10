from datetime import datetime, timedelta

import pytest  # noqa


from libsys_airflow.plugins.vendor.purge import find_directories, remove_archived

vendor_interfaces = [
    {
        "vendor": "8a8dc6dd-8be6-4bd9-80cd-e00409b37dc6",
        "interfaces": {
            "88d39c9c-fa8c-46ee-921d-71f725afb719": "ec1234.mrc",
            "9666e9af-a203-4c38-8708-bda60af8f235": "abcd56679.mrc"
        }
    },
    {
        "vendor": "9cce436e-1858-4c37-9c7f-9374a36576ff",
        "interfaces": {
            "35a42dbe-399f-4292-b2d5-14dd9e0a5e39": "klio71923.mrc",
            "65d30c15-a560-4064-be92-f90e38eeb351": "rt231.mrc"
        }
    }
]


@pytest.fixture
def archive_basepath(tmp_path):
    path = tmp_path / "archive"
    path.mkdir(parents=True)
    return path


def test_find_directories(archive_basepath):
    # Create mock directories
    today = datetime.utcnow()
    prior_90 = today - timedelta(days=90)

    directories = []

    for date in [prior_90 - timedelta(days=1), prior_90]:
        single_archive = archive_basepath / date.strftime("%Y%m%d")
        single_archive.mkdir()
        directories.append(single_archive)

    # Adds today
    directories.append(archive_basepath / today.strftime("%Y%m%d"))
    target_directories = find_directories(archive_basepath)

    assert len(target_directories) == 2
    assert target_directories == directories[0:2]


def test_find_empty_directory(archive_basepath, caplog):
    today = datetime.utcnow()
    today_archive = archive_basepath / today.strftime("%Y%m%d")
    today_archive.mkdir()

    target_directories = find_directories(archive_basepath)

    assert "No directories available for purging" in caplog.text
    assert len(target_directories) == 0


def test_remove_archived(archive_basepath):
    # Create Mocks
    today = datetime.utcnow()
    prior_90 = today - timedelta(days=90)
    target_directory = archive_basepath / prior_90.strftime("%Y%m%d")
    target_directory.mkdir()
    for row in vendor_interfaces:
        vendor_path = target_directory / row["vendor"]
        for interface, file in row["interfaces"].items():
            interface_path = vendor_path / interface
            interface_path.mkdir(parents=True)
            (interface_path / file).touch()

    target_directories = find_directories(archive_basepath)
    result = remove_archived(target_directories)

    assert target_directory.exists() is False
    assert len(result[0]) == 2
    first_vendor = result[0]["8a8dc6dd-8be6-4bd9-80cd-e00409b37dc6"]
    assert first_vendor["date"] == prior_90.strftime("%Y%m%d")
    assert len(first_vendor["interfaces"]) == 2
    interface_marc_file = first_vendor["interfaces"]["88d39c9c-fa8c-46ee-921d-71f725afb719"]
    assert interface_marc_file.startswith("ec1234.mrc")
