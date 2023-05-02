import pytest  # noqa

import os
import shutil

from libsys_airflow.plugins.vendor.archive import archive


@pytest.fixture
def download_path(tmp_path):
    path = os.path.join(tmp_path, "download")
    os.mkdir(path)
    shutil.copyfile("tests/vendor/0720230118.mrc", os.path.join(path, "0720230118.mrc"))
    return path


@pytest.fixture
def archive_path(tmp_path):
    path = os.path.join(tmp_path, "archive")
    os.mkdir(path)
    return path


def test_archive_no_files(download_path, archive_path):
    archive([], download_path, archive_path)

    assert os.listdir(archive_path) == []


def test_archive(download_path, archive_path):
    archive(["0720230118.mrc"], download_path, archive_path)

    assert os.listdir(archive_path) == ["0720230118.mrc"]
