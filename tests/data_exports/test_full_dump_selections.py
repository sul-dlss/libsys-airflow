import pydantic
import pytest

from libsys_airflow.plugins.data_exports import full_dump_marc
from libsys_airflow.plugins.data_exports.marc import exporter


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


class MockS3Operator(pydantic.BaseModel):
    task_id: str
    s3_bucket: str
    s3_key: str
    data: bytes
    replace: bool

    def execute(self, context):
        pass


def mock_number_of_records(mock_result_set):
    return len(mock_result_set)


def mock_marc_records():
    return ""


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


def test_fetch_full_dump(tmp_path, mocker, mock_get_current_context, caplog):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.full_dump_marc.get_current_context",
        return_value={"params": {"batch_size": 3}},
    )

    mocker.patch.object(exporter, "get_current_context", mock_get_current_context)
    mocker.patch.object(full_dump_marc, "SQLExecuteQueryOperator", MockSQLOperator)
    mocker.patch.object(exporter, "S3CreateObjectOperator", MockS3Operator)
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    mocker.patch(
        'libsys_airflow.plugins.data_exports.full_dump_marc.fetch_number_of_records',
        return_value=mock_number_of_records(mock_result_set()),
    )

    full_dump_marc.fetch_full_dump_marc()

    assert "Saving 3 marc records to 0_3.mrc in bucket" in caplog.text
    assert "Saving 3 marc records to 3_6.mrc in bucket" in caplog.text
    assert "Saving 3 marc records to 6_9.mrc in bucket" in caplog.text
    assert "Saving 2 marc records to 9_12.mrc in bucket" in caplog.text
