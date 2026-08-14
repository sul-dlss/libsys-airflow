import pathlib

import pytest  # noqa

from fastapi.testclient import TestClient

from conftest import root_directory

from libsys_airflow.plugins.sdr.apps import sdr_missing_barcodes_view
from libsys_airflow.plugins.sdr.apps.sdr_missing_barcodes_view import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def mock_reports_base(mocker):
    reports_base = pathlib.Path(f"{root_directory}/tests/apps/sdr/report_file_fixtures")
    mocker.patch.object(sdr_missing_barcodes_view, "reports_base", reports_base)
    return reports_base


def test_sdr_missing_barcodes_home():
    response = client.get("/")

    assert response.status_code == 200
    assert "missing-barcodes" in response.text


def test_download():
    response = client.get("/missing-barcodes.csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/csv")
