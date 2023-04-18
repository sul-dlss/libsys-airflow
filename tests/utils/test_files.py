import pytest

from pytest_mock import MockerFixture

from tests.mocks import (
    MockTaskInstance
)

import tests.mocks as mocks


from libsys_airflow.utils.files import (
    backup_retrieved_files,
    check_retrieve_files,
    rename_vendor_files,
    zip_extraction
)


@pytest.fixture
def mock_valid_current_context(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {
            "vendor": "gobi",
            "interface": "34556abcd",
            "file_regex": r"test\.mrc"
        }
        return context

    monkeypatch.setattr(
        "libsys_airflow.utils.files.get_current_context", mock_get_current_context
    )


def test_backup_retrieved_files(caplog):
    mocks.messages = {
        "zip-extract-zipfiles": {
            "return_value": ["test.zip"]
        }
    }

    backup_retrieved_files(
        task_instance=MockTaskInstance(task_id="file-backups"),
        backup_files=["test.mrc", "brief-test.mrc"],
        zip_task_id="zip-extract-zipfiles")

    assert "Backing up test.mrc" in caplog.text
    assert "Backing up test.zip" in caplog.text

    mocks.messages = {}


def test_check_retrieve_files(mock_valid_current_context):

    check_retrieve_files(
        task_instance=MockTaskInstance(task_id="check-count-file")
    )

    interface_path = "/opt/airflow/dataloader/gobi/2023-04-13"
    assert mocks.messages["check-count-file"]["marc-brief-orders"] == f"{interface_path}/sample.mrc"

    mocks.messages = {}


def test_rename_vendor_files():
    mocks.messages["extract-zipfiles"] = {
        "return_value": ["test.zip"]
    }

    rename_vendor_files(
        files=["sample.mrc", "brief.mrc"],
        task_instance=MockTaskInstance(task_id="rename-files")
    )

    assert mocks.messages["rename-files"]["files"] == ['sample.mrc0', 'brief.mrc1', 'test.zip2']

    mocks.messages = {}


def test_zip_extraction(tmp_path):
    zip_file_path = tmp_path / "test.zip"
    return_files = zip_extraction(
        task_instance=MockTaskInstance(task_id="extract-zipfiles"),
        zipfile=str(zip_file_path)
    )

    assert "sample-01.mrc" in return_files[0]
    assert "sample-02.mrc" in return_files[1]
