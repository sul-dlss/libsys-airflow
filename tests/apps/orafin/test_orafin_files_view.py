import pathlib

import pytest  # noqa

from fastapi.testclient import TestClient

from auth_helpers import authenticate  # noqa
from conftest import root_directory

from libsys_airflow.plugins.orafin.apps import orafin_files_view
from libsys_airflow.plugins.orafin.apps.orafin_files_view import app

authenticate(app)
client = TestClient(app)


@pytest.fixture(autouse=True)
def mock_files_base(mocker):
    files_base = pathlib.Path(
        f"{root_directory}/tests/apps/orafin/orafin_file_fixtures"
    )
    mocker.patch.object(orafin_files_view, "files_base", files_base)
    return files_base


def test_orafin_files_home():
    response = client.get("/")
    assert response.status_code == 200

    assert 'href="data/feeder20241130_20241218"' in response.text
    assert response.text.count('href="reports/') == 3


def test_downloads():
    response = client.get("/data/feeder20241130_20241218")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/text")

    csv_response = client.get("/reports/xxdl_ap_payments_1218202453000.csv")
    assert csv_response.status_code == 200
    assert csv_response.headers["content-type"].startswith("application/csv")
