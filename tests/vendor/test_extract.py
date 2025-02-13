import pytest  # noqa

import os
import shutil
import pathlib

from libsys_airflow.plugins.vendor.extract import _is_tar, extract


@pytest.fixture
def download_path(tmp_path):
    shutil.copyfile(
        "tests/vendor/0720230118.tar.gz", os.path.join(tmp_path, "0720230118.tar.gz")
    )
    shutil.copyfile(
        "tests/vendor/download.tar.gz", os.path.join(tmp_path, "download.tar.gz")
    )
    return tmp_path


def test_is_tar():
    assert not _is_tar(pathlib.Path("tests/vendor/0720230118.mrc"))
    assert _is_tar(pathlib.Path("tests/vendor/0720230118.tar.gz"))


def test_extract_no_regex(download_path):
    extracted_filename = extract(
        pathlib.Path(download_path) / "0720230118.tar.gz", None
    )
    assert extracted_filename == "0720230118.mrc"

    assert (pathlib.Path(download_path) / "0720230118.mrc").is_file()


def test_extract_regex(download_path):
    extracted_filename = extract(
        pathlib.Path(download_path) / "download.tar.gz", r"^.*\.EDI$"
    )
    assert extracted_filename == "AuxamInvoice220324676717.EDI"

    assert (pathlib.Path(download_path) / "AuxamInvoice220324676717.EDI").is_file()


def test_extract_no_file(download_path):
    with pytest.raises(ValueError):
        extract(pathlib.Path(download_path) / "0720230118.tar.gz", r"^.*\.EDI$")


def test_extract_multiple_files(download_path):
    with pytest.raises(ValueError):
        extract(pathlib.Path(download_path) / "download.tar.gz", None)
