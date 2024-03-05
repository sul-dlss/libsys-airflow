import pytest  # noqa

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    connection_details_task,
    gather_files_task,
    transmit_data_task,
    archive_transmitted_data_task,
)


def test_gather_files_task(tmp_path):
    airflow = tmp_path / "airflow"
    data_exports = airflow / "data-export-files/oclc/marc-files/"
    data_exports.mkdir(parents=True)
    marc_file = data_exports / "2024022914.mrc"
    marc_file.touch()
    marc_file_list = gather_files_task.function(airflow=airflow, vendor="oclc")
    assert marc_file_list == [str(marc_file)]
