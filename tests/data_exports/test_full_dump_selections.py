import csv
import pathlib
import pydantic
import pytest

from libsys_airflow.plugins.data_exports import full_dump_ids


class MockSQLOperator(pydantic.BaseModel):
    task_id: str
    conn_id: str
    database: str
    sql: str
    parameters: dict

    def execute(self, context):
        mock_result = mock_result_set()
        start = self.parameters["offset"]
        end = start + self.parameters["limit"]
        return mock_result[start:end]


def mock_number_of_records(mock_result_set):
    return len(mock_result_set)


def mock_result_set():
    return [
        ['4e66ce0d-4a1d-41dc-8b35-0914df20c7fb'],
        ['fe2e581f-9767-442a-ae3c-a421ac655fe2'],
        ['fde293e9-f4fa-4236-b9db-2cef7d57d5ee'],
        ['189dda6e-fa87-41ee-9672-d39bef3dda20'],
        ['5d278b69-57b7-4dcd-ba18-62dda2c05b20'],
        ['d90e8f77-421f-4ed0-8e46-9563be80eee4'],
        ['3e35d5a8-2f2e-4b67-bf26-5cfbf341e96a'],
        ['5b8f26de-5773-4f21-ae37-7ab036f8d5e1'],
        ['5ec79836-ef37-4388-b1c3-680c8a0b7cac'],
        ['90e08427-6cef-493a-b1ab-11ec3a8f340d'],
        ['3dcd0768-aa4a-46c8-89ce-e952b9a01205'],
    ]


@pytest.fixture
def mock_get_current_context(mocker):
    context = mocker.stub(name="context")
    context.get = lambda arg: {}
    return context


def test_fetch_full_dump_ids(tmp_path, mocker, mock_get_current_context):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.full_dump_ids.get_current_context",
        return_value={},
    )

    mocker.patch.object(full_dump_ids, "SQLExecuteQueryOperator", MockSQLOperator)

    mocker.patch(
        'libsys_airflow.plugins.data_exports.full_dump_ids.fetch_number_of_records',
        return_value=mock_number_of_records(mock_result_set()),
    )

    full_dump_ids.fetch_full_dump_ids(airflow=tmp_path, batch_size=3)

    files_dir = pathlib.Path(tmp_path) / "data-export-files/full-dump/instanceids"
    csv_files = [i for i in files_dir.glob("*.csv")]

    assert len(csv_files) == 4

    csv_files = sorted(csv_files)

    with csv_files[3].open('r') as fo:
        id_list = list(row for row in csv.reader(fo))

    assert id_list[0][0] == "90e08427-6cef-493a-b1ab-11ec3a8f340d"
