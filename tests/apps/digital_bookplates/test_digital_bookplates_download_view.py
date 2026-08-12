import pathlib

from fastapi.testclient import TestClient
import pytest  # noqa

from conftest import root_directory

from libsys_airflow.plugins.digital_bookplates.apps import (
    digital_bookplates_download_view,
)
from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_download_view import (
    app,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def mock_files_base(mocker):
    files_base = pathlib.Path(
        f"{root_directory}/tests/apps/digital_bookplates/digital_bookplates_file_fixtures"
    )
    mocker.patch.object(digital_bookplates_download_view, "files_base", files_base)
    return files_base


def test_download_view():
    response = client.get('/')
    assert response.status_code == 200

    assert (
        'id="2024-10-22" href="2024/10/22/SearchInstanceUUIDs2024-10-16T16_39_11-06_00.csv"'
        in response.text
    )
