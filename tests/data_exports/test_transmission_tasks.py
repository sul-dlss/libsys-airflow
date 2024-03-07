import pytest  # noqa
import pathlib

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    gather_files_task,
    archive_transmitted_data_task,
)


@pytest.fixture
def mock_file_system(tmp_path):
    airflow = tmp_path / "airflow"
    vendor_dir = airflow / "data-export-files/oclc/"
    marc_file_dir = vendor_dir / "marc-files"
    marc_file_dir.mkdir(parents=True)
    instance_id_dir = vendor_dir / "instanceids"
    instance_id_dir.mkdir()
    archive_dir = vendor_dir / "transmitted"
    archive_dir.mkdir()

    return [airflow, marc_file_dir, instance_id_dir, archive_dir]


# Mock xcom messages dict
@pytest.fixture
def mock_marc_file_list(mock_file_system):
    marc_file_dir = mock_file_system[1]
    setup_marc_files = {
        "marc_file_list": ["2024022914.mrc", "2024030114.mrc", "2024030214.mrc"]
    }
    marc_file_list = []
    for x in setup_marc_files['marc_file_list']:
        marc_file = pathlib.Path(f"{marc_file_dir}/{x}")
        marc_file.touch()
        marc_file_list.append(str(marc_file))

    return marc_file_list


def test_gather_files_task(mock_file_system, mock_marc_file_list):
    airflow = mock_file_system[0]
    marc_file_list = gather_files_task.function(airflow=airflow, vendor="oclc")
    assert marc_file_list.sort() == mock_marc_file_list.sort()


def test_archive_transmitted_data_task(mock_file_system, mock_marc_file_list):
    instance_id_dir = mock_file_system[2]
    instance_id_file1 = instance_id_dir / "2024022914.csv"
    instance_id_file1.touch()
    archive_transmitted_data_task.function(mock_marc_file_list)
    assert not instance_id_file1.exists()
    transmitted_dir = mock_file_system[-1]
    for x in mock_marc_file_list:
        assert (transmitted_dir / pathlib.Path(x).name).exists()

    assert (transmitted_dir / instance_id_file1.name).exists()


def test_archive_transmitted_data_task_no_files(caplog):
    archive_transmitted_data_task.function([])
    assert "No files to archive" in caplog.text
