import pytest  # noqa
import pydantic

from airflow.models import Connection

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.reading_room import (
    generate_reading_room_access,
)


class MockReadingRoomsData:
    def __init__(self):
        self.patron_groups = {
            "b1f10c81-01c1-4b97-9362-6d412df42f52": "faculty",
            "c2f10c81-01c1-4b97-9362-6d412df42f53": "courtesy",
            "d3f10c81-01c1-4b97-9362-6d412df42f54": "graduate",
            "e4f10c81-01c1-4b97-9362-6d412df42f55": "pseudopatron",
        }
        self.usergroups = {
            "opt_1": "sul - borrowdirect brown",
            "opt_2": "sul - university librarian guest",
            "opt_99": "best friend",
        }
        self.reading_rooms = {
            "Green 24-hour study space": "95d15527-7bd4-42e7-a629-0618742918e5",
            "Reading Room A": "a4d15527-7bd4-42e7-a629-0618742918e6",
        }


class MockPsycopg2Cursor(pydantic.BaseModel):
    def fetchall(self):
        return [()]

    def execute(self, sql_stmt, params):
        self


class MockPsycopg2Connection(pydantic.BaseModel):
    def cursor(self):
        return MockPsycopg2Cursor()


@pytest.fixture
def mock_airflow_connection():
    return Connection(  # noqa
        conn_id="postgres-folio",
        conn_type="postgres",
        host="example.com",
        password="pass",
        port=9999,
    )


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        if args[0] == "reading-room-patron-permission/user_1":
            return [
                {
                    "id": "perm_user_1",
                    "userId": "user_1",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "NOT_ALLOWED",
                    "metadata": {
                        "createdDate": "2025-12-16 17:40:52",
                        "createdByUserId": "84109a43-8ad2-482e-bb8e-b0675f57461c",
                        "updatedDate": "2025-12-16 21:19:46",
                        "updatedByUserId": "58d0aaf6-dcda-4d5e-92da-012e6b7dd766",
                    },
                },
            ]
        if args[0] == "reading-room-patron-permission/user_4":
            return [
                {
                    "id": "perm_user_4",
                    "userId": "user_4",
                    "readingRoomId": "a4d15527-7bd4-42e7-a629-0618742918e6",
                    "readingRoomName": "Reading Room A",
                    "access": "ALLOWED",
                    "metadata": {
                        "createdDate": "2025-12-16 17:40:52",
                        "createdByUserId": "84109a43-8ad2-482e-bb8e-b0675f57461c",
                        "updatedDate": "2025-12-16 21:19:46",
                        "updatedByUserId": "58d0aaf6-dcda-4d5e-92da-012e6b7dd766",
                    },
                }
            ]
        return []

    def mock_get_all(*args, **kwargs):
        return []

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    mock_client.folio_get_all = mock_get_all
    return mock_client


@pytest.fixture
def users():
    return [
        {
            "id": "user_1",  # NOT_ALLOWED based on usergroup
            "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
            "customFields": {"usergroup": "opt_1"},  # sul - borrowdirect brown
        },
        {
            "id": "user_2",  # NOT_ALLOWED based on usergroup
            "patronGroup": "c2f10c81-01c1-4b97-9362-6d412df42f53",  # courtesy
            "customFields": {"usergroup": "opt_2"},  # sul - university librarian guest
        },
        {
            "id": "user_3",  # ALLOWED
            "patronGroup": "d3f10c81-01c1-4b97-9362-6d412df42f54",  # graduate
            "customFields": {"usergroup": "opt_99"},  # best friend
        },
        {
            "id": "user_4",  # NOT_ALLOWED based on patron group
            "patronGroup": "e4f10c81-01c1-4b97-9362-6d412df42f55",  # pseudopatron
            "customFields": {"usergroup": "opt_99"},  # best friend
        },
    ]


@pytest.fixture
def mock_reading_rooms_config(monkeypatch):
    config = {
        "Green 24-hour study space": {
            "allowed": {
                "patron_group_name": [
                    "courtesy",
                    "faculty",
                    "graduate",
                ]
            },
            "disallowed": {
                "usergroup_name": [
                    "sul - borrowdirect brown",
                    "sul - university librarian guest",
                    "sul - visiting scholar short-term",
                ],
            },
        },
        "Reading Room A": {
            "allowed": {
                "patron_group_name": [
                    "faculty",
                    "graduate",
                ]
            },
            "disallowed": {
                "usergroup_name": [
                    "sul - borrowdirect brown",
                ],
            },
        },
    }

    monkeypatch.setattr(
        'libsys_airflow.plugins.folio.reading_room.reading_rooms_config',
        config,
    )


def test_reading_room_user_permissions(
    mocker,
    users,
    mock_folio_client,
    mock_reading_rooms_config,
    mock_airflow_connection,
):
    mocker.patch(
        "libsys_airflow.plugins.folio.reading_room.folio_client",
        return_value=mock_folio_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.folio.reading_room.ReadingRoomsData",
        return_value=MockReadingRoomsData(),
    )

    reading_room_access = generate_reading_room_access.function(
        users, MockReadingRoomsData()
    )
    assert reading_room_access[0]["permissions"][0]["access"] == "NOT_ALLOWED"  # user_1
    assert reading_room_access[1]["permissions"][0]["access"] == "NOT_ALLOWED"  # user_2
    assert reading_room_access[2]["permissions"][0]["access"] == "ALLOWED"  # user_3
    assert reading_room_access[3]["permissions"][1]["access"] == "NOT_ALLOWED"  # user_4
