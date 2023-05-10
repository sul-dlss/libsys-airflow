from datetime import datetime, timedelta
import pathlib

import pytest  # noqa


from libsys_airflow.plugins.vendor.purge import find_directories

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



