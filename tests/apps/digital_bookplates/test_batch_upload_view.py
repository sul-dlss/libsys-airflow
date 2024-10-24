import datetime
import io

import pandas as pd
import pytest

from airflow.www import app as application
from conftest import root_directory
from bs4 import BeautifulSoup
from flask.wrappers import Response

from pytest_mock_resources import create_sqlite_fixture, Rows

from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_batch_upload_view import (
    DigitalBookplatesBatchUploadView,
    _save_uploaded_file,
)
from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate


rows = Rows(
    DigitalBookplate(
        id=1,
        created=datetime.datetime(2024, 10, 14, 12, 15, 0, 733715),
        updated=datetime.datetime(2024, 10, 14, 12, 15, 0, 733715),
        druid="kp761xz4568",
        fund_name="ASHENR",
        image_filename="dp698zx8237_00_0001.jp2",
        title="Ruth Geraldine Ashen Memorial Book Fund",
        fund_uuid="08cc33e4-228b-4bcd-ae91-53ecb7aa2310",
    ),
    DigitalBookplate(
        id=2,
        created=datetime.datetime(2024, 10, 14, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 10, 14, 17, 16, 15, 986798),
        druid="ab123xy4567",
        fund_name=None,
        image_filename="ab123xy4567_00_0001.jp2",
        title="Alfred E. Newman Magazine Fund for Humor Studies",
        fund_uuid=None,
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def test_airflow_client():
    templates_folder = (
        f"{root_directory}/libsys_airflow/plugins/digital_bookplates/templates"
    )

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False
    app.appbuilder.add_view(
        DigitalBookplatesBatchUploadView,
        "DigitalBookplatesBatchUpload",
        category="FOLIO",
    )
    app.blueprints['DigitalBookplatesBatchUploadView'].template_folder = (
        templates_folder
    )
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_digital_bookplates_batch_upload_view(test_airflow_client, mock_db):
    response = test_airflow_client.get('/digital_bookplates_batch_upload/')

    assert response.status_code == 200

    funds_list = response.html.find(id="fundsTable").find("tbody").find_all("tr")

    assert len(funds_list) == 2


def test_missing_filename(test_airflow_client, mock_db):
    response = test_airflow_client.post('/digital_bookplates_batch_upload/create')

    assert response.status_code == 302

    redirect_response = test_airflow_client.get('/digital_bookplates_batch_upload/')

    alert = redirect_response.html.find(class_="alert-message").get_text()

    assert "Missing Instance UUIDs file" in alert


def test_get_fund(mocker, mock_db, tmp_path):
    mocker.patch.object(DigitalBookplatesBatchUploadView, "files_base", tmp_path)

    from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_batch_upload_view import (
        _get_fund,
    )

    fund = _get_fund(1)
    assert fund == {
        'druid': 'kp761xz4568',
        'fund_name': 'ASHENR',
        'image_filename': 'dp698zx8237_00_0001.jp2',
        'title': 'Ruth Geraldine Ashen Memorial Book Fund',
    }


def test_upload_file(mocker, test_airflow_client, mock_db, tmp_path):
    mocker.patch("libsys_airflow.plugins.digital_bookplates.bookplates.DagBag")

    mocker.patch.object(DigitalBookplatesBatchUploadView, "files_base", tmp_path)

    response = test_airflow_client.post(
        '/digital_bookplates_batch_upload/create',
        data={
            "email": "test@stanford.edu",
            "fundSelect": 1,
            "upload-instance-uuids": (
                io.BytesIO(b"4670950c-a01a-428c-ba2f-f0bf539665f7"),
                "upload-file.csv",
            ),
        },
    )

    assert response.status_code == 302

    redirect_response = test_airflow_client.get('/digital_bookplates_batch_upload/')

    alert = redirect_response.html.find(class_="alert-message").get_text()

    assert "Triggered the following DAGs" in alert


def test_existing_upload_file(tmp_path):
    current_timestamp = datetime.datetime.utcnow()
    upload_path = (
        tmp_path
        / f"{current_timestamp.year}/{current_timestamp.month}/{current_timestamp.day}"
    )
    upload_path.mkdir(parents=True, exist_ok=True)
    existing_file = upload_path / "new-bookplate-instances.csv"
    existing_file.touch()

    instance_uuids_df = pd.DataFrame(["75375cc1-c796-44ea-aa82-af372540cea1"])
    _save_uploaded_file(tmp_path, "new-bookplate-instances.csv", instance_uuids_df)

    assert (upload_path / "new-bookplate-instances-copy-1.csv").exists()

    _save_uploaded_file(tmp_path, "new-bookplate-instances.csv", instance_uuids_df)

    assert (upload_path / "new-bookplate-instances-copy-2.csv").exists()
