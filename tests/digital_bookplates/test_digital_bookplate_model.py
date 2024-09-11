import datetime

import pytest  # noqa
from pytest_mock_resources import create_sqlite_fixture, Rows

from sqlalchemy.orm import Session

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate

from airflow.providers.postgres.hooks.postgres import PostgresHook

rows = Rows(
    DigitalBookplate(
        id=1,
        created=datetime.datetime(2024, 9, 9, 12, 15, 0, 733715),
        updated=datetime.datetime(2024, 9, 9, 12, 15, 0, 733715),
        druid="kp761xz4568",
        name="ASHENR",
        image_filename="dp698zx8237_00_0001.jp2",
        title="Ruth Geraldine Ashen Memorial Book Fund",
    ),
    DigitalBookplate(
        id=2,
        created=datetime.datetime(2024, 9, 10, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 10, 17, 16, 15, 986798),
        druid="gc698jf6425",
        image_filename="gc698jf6425_00_0001.jp2",
        name="RHOADES",
        title="John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
    ),
    DigitalBookplate(
        id=3,
        created=datetime.datetime(2024, 9, 11, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 11, 17, 16, 15, 986798),
        druid="ab123xy4567",
        name=None,
        image_filename="ab123xy4567_00_0001.jp2",
        title="Alfred E. Newman Magazine Fund for Humor Studies",
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def pg_hook(mocker, engine) -> PostgresHook:
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


def test_digital_bookplates_retrival(pg_hook):
    with Session(pg_hook()) as session:
        book_plates = session.query(DigitalBookplate).all()

        assert book_plates[0].name == "ASHENR"
        assert book_plates[1].druid == "gc698jf6425"
        assert book_plates[2].name is None
        assert book_plates[2].title.startswith(
            "Alfred E. Newman Magazine Fund for Humor Studies"
        )
