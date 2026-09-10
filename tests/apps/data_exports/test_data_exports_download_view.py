import pathlib

from fastapi.testclient import TestClient
import pytest  # noqa

from auth_helpers import authenticate  # noqa
from conftest import root_directory

from libsys_airflow.plugins.data_exports.apps import data_export_download_view
from libsys_airflow.plugins.data_exports.apps.data_export_download_view import app

authenticate(app)
client = TestClient(app)


@pytest.fixture(autouse=True)
def mock_files_base(mocker):
    files_base = pathlib.Path(
        f"{root_directory}/tests/apps/data_exports/data_export_file_fixtures"
    )
    mocker.patch.object(data_export_download_view, "files_base", files_base)
    return files_base


def test_download_view():
    response = client.get('/')
    assert response.status_code == 200

    assert (
        'class="oclc-marc-files-new" href="downloads/oclc/marc-files/new/202003131720.mrc"'
        in response.text
    )
    assert (
        'class="oclc-transmitted-deletes" href="downloads/oclc/transmitted/deletes/202103131720.mrc"'
        in response.text
    )
