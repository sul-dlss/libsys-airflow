from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest

from fastapi.testclient import TestClient
from auth_helpers import authed_test_client  # noqa
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SQLAlchemySession
from sqlalchemy.pool import StaticPool

from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_batch_upload_view import (
    app,
    _save_uploaded_file,
    _get_fund,
)
from libsys_airflow.plugins.digital_bookplates.apps import (
    digital_bookplates_batch_upload_view,
)
from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate, Model

from mocks import (  # noqa
    MockAirflowApiClientConfig,
    MockAirflowApiClient,
)


client = authed_test_client(app, follow_redirects=False)


@pytest.fixture
def engine():
    # FastAPI's TestClient dispatches requests on a background thread, so the
    # sqlite connection must be shared (StaticPool) and thread-unsafe checks
    # disabled, otherwise the view's DB session can't see the seeded rows.
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Model.metadata.create_all(engine)
    with SQLAlchemySession(engine) as session:
        session.add_all(
            [
                DigitalBookplate(
                    id=1,
                    created=datetime(2024, 10, 14, 12, 15, 0, 733715),
                    updated=datetime(2024, 10, 14, 12, 15, 0, 733715),
                    druid="kp761xz4568",
                    fund_name="ASHENR",
                    image_filename="dp698zx8237_00_0001.jp2",
                    title="Ruth Geraldine Ashen Memorial Book Fund",
                    fund_uuid="08cc33e4-228b-4bcd-ae91-53ecb7aa2310",
                ),
                DigitalBookplate(
                    id=2,
                    created=datetime(2024, 10, 14, 17, 16, 15, 986798),
                    updated=datetime(2024, 10, 14, 17, 16, 15, 986798),
                    druid="ab123xy4567",
                    fund_name=None,
                    image_filename="ab123xy4567_00_0001.jp2",
                    title="Alfred E. Newman Magazine Fund for Humor Studies",
                    fund_uuid=None,
                ),
            ]
        )
        session.commit()
    return engine


@pytest.fixture
def mock_client_config():
    return MockAirflowApiClientConfig()


@pytest.fixture
def mock_api_client():
    return MockAirflowApiClient(configuration=MockAirflowApiClientConfig())


def mock_api_instance():
    api_instance = MagicMock()

    mock_response = MagicMock()
    mock_response.dag_id = "digital_bookplate_979"
    mock_response.dag_run_id = "manual__2024-10-17"

    api_instance.trigger_dag_run.return_value = mock_response

    return api_instance


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


def test_digital_bookplates_batch_upload_view(mock_db):
    response = client.get('/')

    assert response.status_code == 200
    assert "kp761xz4568" in response.text
    assert "ab123xy4567" in response.text


def test_missing_filename(mock_db):
    response = client.post('/create')

    assert response.status_code == 303

    redirect_response = client.get(response.headers["location"])

    assert "Missing Instance UUIDs file" in redirect_response.text


def test_get_fund(mocker, mock_db, tmp_path):
    mocker.patch.object(digital_bookplates_batch_upload_view, "files_base", tmp_path)

    fund = _get_fund(1)
    assert fund == {
        'druid': 'kp761xz4568',
        'fund_name': 'ASHENR',
        'image_filename': 'dp698zx8237_00_0001.jp2',
        'title': 'Ruth Geraldine Ashen Memorial Book Fund',
    }


def test_upload_file(mocker, mock_api_client, mock_db, tmp_path):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.DagRunApi",
        return_value=mock_api_instance(),
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.api_client",
        return_value=mock_api_client,
    )
    mocker.patch.object(digital_bookplates_batch_upload_view, "files_base", tmp_path)

    response = client.post(
        '/create',
        data={"email": "test@stanford.edu", "fund_select": 1},
        files={
            "upload_instance_uuids": (
                "upload-file.csv",
                BytesIO(b"4670950c-a01a-428c-ba2f-f0bf539665f7"),
                "text/csv",
            )
        },
    )

    assert response.status_code == 303

    redirect_response = client.get(response.headers["location"])

    assert "Triggered 1 DAG run(s)" in redirect_response.text


def test_existing_upload_file(tmp_path):
    current_timestamp = datetime.now(timezone.utc)
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


def test_column_header(mocker, mock_api_client, mock_db, tmp_path):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.DagRunApi",
        return_value=mock_api_instance(),
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.api_client",
        return_value=mock_api_client,
    )
    mocker.patch.object(digital_bookplates_batch_upload_view, "files_base", tmp_path)

    client.post(
        '/create',
        data={"email": "test@stanford.edu", "fund_select": 1},
        files={
            "upload_instance_uuids": (
                "upload-file.csv",
                BytesIO(b"4670950c-a01a-428c-ba2f-f0bf539665f7"),
                "text/csv",
            )
        },
    )

    current_timestamp = datetime.now(timezone.utc)
    upload_path = (
        tmp_path
        / f"{current_timestamp.year}/{current_timestamp.month}/{current_timestamp.day}"
    )
    file = upload_path / "upload-file.csv"
    assert (file).exists()

    with open(file) as f:
        first_line = f.readline()
        assert first_line == 'Instance UUID\n'


def test_trigger_add_979_dags_without_csrf_token():
    response = TestClient(app, follow_redirects=False).post("/create")

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF token missing or invalid"
