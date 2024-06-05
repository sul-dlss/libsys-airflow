import pathlib
import pytest
import os

from datetime import datetime, timedelta
from libsys_airflow.plugins.shared.purge import find_files, remove_files
from tests.data_exports.test_transmission_tasks import mock_file_system  # noqa


@pytest.fixture
def mock_vendor_transmitted_files(tmp_path):
    airflow = tmp_path / "airflow"
    vendor_dir = airflow / "data-export-files/some-vendor/"
    marc_file_dir = vendor_dir / "transmitted" / "updates"
    setup_files = [
        "2024022914.mrc",
        "2024030114.mrc",
        "2024030214.mrc",
        "2024030214.txt",
    ]
    files = []
    for f in setup_files:
        file = pathlib.Path(f"{marc_file_dir}/{f}")
        file.touch()
        files.append(str(file))

    return files


@pytest.fixture
def mock_prior_days_files(mock_vendor_transmitted_files):
    files = mock_vendor_transmitted_files
    prior_timestamp = (datetime.utcnow() - timedelta(days=180)).timestamp()
    for f in files:
        os.utime(f, (prior_timestamp, prior_timestamp))

    return files


def test_files_not_removed(mock_file_system):  # noqa
    files = find_files(downloads_directory=mock_file_system[-1])
    assert len(files) == 0


def test_files_removed(mock_file_system, mock_prior_days_files, caplog):  # noqa
    files = find_files(downloads_directory=mock_file_system[-1])
    remove_files(target_files=files)
    for f in files:
        assert f"Removed {f}" in caplog.text
