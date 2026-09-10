import pathlib

from fastapi.testclient import TestClient
import pytest  # noqa

from auth_helpers import authenticate  # noqa
from conftest import root_directory

from libsys_airflow.plugins.data_exports.apps import data_export_oclc_reports_view
from libsys_airflow.plugins.data_exports.apps.data_export_oclc_reports_view import app

authenticate(app)
client = TestClient(app)


@pytest.fixture(autouse=True)
def mock_files_base(mocker):
    files_base = pathlib.Path(
        f"{root_directory}/tests/apps/data_exports/data_export_file_fixtures"
    )
    mocker.patch.object(data_export_oclc_reports_view, "files_base", files_base)
    return files_base


def test_oclc_reports_view():
    response = client.get('/')
    assert response.status_code == 200

    assert "<h3>Graduate School of Business</h3>" in response.text
    assert 'href="S7Z/new_marc_errors/2024-09-13T16:01:18.963349.html"' in response.text

    assert "<h3>Stanford University Libraries</h3>" in response.text
    assert 'href="STF/unset_holdings/2024-09-13T15:47:28.857056.html"' in response.text
