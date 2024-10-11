import datetime
import pytest

from airflow.www import app as application
from conftest import root_directory
from bs4 import BeautifulSoup
from flask.wrappers import Response

from pytest_mock_resources import create_sqlite_fixture, Rows

from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_batch_upload_view import (
    DigitalBookplatesBatchUploadView,
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
        fund_uuid="f2421e3f-0089-497b-a3c6-104256a5c4ff",
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


    funds_list = response.html.find(id="fundsTable").find_all('tbody.tr')

    assert len(funds_list) == 2
    
