import pytest  # noqa

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    connection_details_task,
    gather_files_task,
    transmit_data_task,
    archive_transmitted_data_task,
)


# Mock xcom messages dict
setup_marc_files = {
    "marc_file_list": ["2024022914.mrc", "2024030114.mrc", "2024030214.mrc"],
}


@pytest.fixture
def mock_file_system(tmp_path):
    airflow = tmp_path / "airflow"
    vendor_dir = airflow / "data-export-files/oclc/"
    marc_file_dir = vendor_dir / "marc-files"
    marc_file_dir.mkdir(parents=True)

    archive_dir = vendor_dir / "archive"

    return [airflow, marc_file_dir, archive_dir]


@pytest.fixture
def mock_marc_file_list(mock_file_system):
    marc_file_dir = mock_file_system[1]
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

