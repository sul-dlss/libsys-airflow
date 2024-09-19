import datetime
import pytest  # noqa

from unittest.mock import MagicMock

from airflow.providers.postgres.hooks.postgres import PostgresHook
from pytest_mock_resources import create_sqlite_fixture, Rows

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from libsys_airflow.plugins.digital_bookplates.bookplates import bookplate_fund_ids

rows = Rows(
    DigitalBookplate(
        id=1,
        created=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        updated=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        druid="kp761xz4568",
        fund_name="ASHENR",
        image_filename="dp698zx8237_00_0001.jp2",
        title="Ruth Geraldine Ashen Memorial Book Fund",
    ),
    DigitalBookplate(
        id=2,
        created=datetime.datetime(2024, 9, 12, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 12, 17, 16, 15, 986798),
        druid="gc698jf6425",
        image_filename="gc698jf6425_00_0001.jp2",
        fund_name="RHOADES",
        title="John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
    ),
    DigitalBookplate(
        id=3,
        created=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        druid="ab123xy4567",
        fund_name=None,
        image_filename="ab123xy4567_00_0001.jp2",
        title="Alfred E. Newman Magazine Fund for Humor Studies",
    ),
)

engine = create_sqlite_fixture(rows)

funds = {
    "funds": [
        {
            "id": "b8932bcd-7498-4f7e-a598-de9010561e42",
            "name": "ASHENR",
        },
        {
            "id": "06220dd4-7d6e-4e5b-986d-5fca21d856ca",
            "name": "RHOADES",
        },
        {
            "id": "a038f042-ee9e-44ef-bf0d-b7eacd5225bc",
            "name": "NONE",
        },
    ]
}


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        return funds

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    return mock_client


@pytest.fixture
def pg_hook(mocker, engine) -> PostgresHook:
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


def test_bookplate_fund_ids(mocker, pg_hook, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates._folio_client",
        return_value=mock_folio_client,
    )

    assert bookplate_fund_ids.function() == {
        "kp761xz4568": "b8932bcd-7498-4f7e-a598-de9010561e42",
        "gc698jf6425": "06220dd4-7d6e-4e5b-986d-5fca21d856ca",
    }
