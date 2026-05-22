import pytest
from datetime import datetime
from unittest.mock import MagicMock, mock_open, patch

from libsys_airflow.plugins.folio.reading_room import (
    retrieve_usergroup_lookup,
    retrieve_patron_group_lookup,
    retrieve_reading_rooms_lookup,
    retrieve_users_batch_for_reading_room_access,
    generate_reading_room_access,
    update_reading_room_permissions,
    formatted_date,
    get_usergroup_sql_path,
)


@pytest.fixture
def mock_postgres_hook():
    """Mock PostgresHook for usergroup lookup"""
    mock_hook = MagicMock()
    # The SQL returns a single column with JSONB array of objects
    # Structure: [(jsonb_array,)]
    mock_hook.get_records.return_value = [
        (
            [  # This is the JSONB array from selectField->options->values
                {"id": "opt_1", "value": "sul - borrowdirect brown"},
                {"id": "opt_2", "value": "sul - university librarian guest"},
                {"id": "opt_99", "value": "best friend"},
            ],
        )
    ]
    return mock_hook


@pytest.fixture
def mock_reading_rooms_config(monkeypatch):
    """Mock reading rooms configuration"""
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
        "libsys_airflow.plugins.folio.reading_room.reading_rooms_config",
        config,
    )
    return config


@pytest.fixture
def sample_users():
    """Sample user data for testing"""
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
def lookup_data():
    """Lookup dictionaries for testing"""
    return {
        "usergroups": {
            "opt_1": "sul - borrowdirect brown",
            "opt_2": "sul - university librarian guest",
            "opt_99": "best friend",
        },
        "patron_groups": {
            "b1f10c81-01c1-4b97-9362-6d412df42f52": "faculty",
            "c2f10c81-01c1-4b97-9362-6d412df42f53": "courtesy",
            "d3f10c81-01c1-4b97-9362-6d412df42f54": "graduate",
            "e4f10c81-01c1-4b97-9362-6d412df42f55": "pseudopatron",
        },
        "reading_rooms": {
            "Green 24-hour study space": "95d15527-7bd4-42e7-a629-0618742918e5",
            "Reading Room A": "a4d15527-7bd4-42e7-a629-0618742918e6",
        },
    }


# Test utility functions
def test_get_usergroup_sql_path():
    """Test SQL path generation"""
    path = get_usergroup_sql_path(airflow="/custom/airflow")
    assert "/custom/airflow/libsys_airflow/plugins/folio/helpers/usergroups.sql" in str(
        path
    )


def test_formatted_date_with_date():
    """Test date formatting with provided date - returns it as-is"""
    result = formatted_date("2026-05-20")
    assert result == "2026-05-20"


def test_formatted_date_with_full_iso_date():
    """Test date formatting with full ISO date - returns it as-is"""
    result = formatted_date("2026-05-20T10:30:00")
    assert result == "2026-05-20T10:30:00"


@patch("libsys_airflow.plugins.folio.reading_room.datetime")
def test_formatted_date_without_date(mock_datetime):
    """Test date formatting without provided date - uses yesterday at midnight"""
    # Mock datetime.now() to return a specific date
    mock_now = datetime(2026, 5, 8, 15, 30, 45, 123456)  # May 8, 2026 at 3:30:45 PM
    mock_datetime.now.return_value = mock_now
    # Need to keep the real datetime class for timedelta operations
    mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

    result = formatted_date(None)

    # Should be yesterday (May 7) at midnight in ISO format
    assert result == "2026-05-07T00:00:00"


@patch("libsys_airflow.plugins.folio.reading_room.datetime")
def test_formatted_date_none_value(mock_datetime):
    """Test that None explicitly triggers yesterday's date"""
    mock_now = datetime(2026, 1, 1, 12, 0, 0)  # Jan 1, 2026 at noon
    mock_datetime.now.return_value = mock_now
    mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

    result = formatted_date(None)

    # Should be Dec 31, 2025 at midnight
    assert result == "2025-12-31T00:00:00"


def test_formatted_date_empty_string():
    """Test that empty string is treated as no date provided"""
    # Empty string is falsy, so should behave like None
    result = formatted_date("")

    # Should be in ISO format with T00:00:00
    assert "T00:00:00" in result
    # Should not have microseconds
    assert "." not in result


def test_formatted_date_preserves_time():
    """Test that if a datetime string with time is provided, it's preserved"""
    input_date = "2026-05-20T14:30:00"
    result = formatted_date(input_date)
    assert result == "2026-05-20T14:30:00"


# Test lookup tasks
@patch('libsys_airflow.plugins.folio.reading_room.PostgresHook')
def test_retrieve_usergroup_lookup(mock_hook_class, mock_postgres_hook):
    """Test usergroup lookup retrieval"""
    mock_hook_class.return_value = mock_postgres_hook

    sql_content = """SELECT jsonb->'selectField'->'options'->'values'
FROM sul_mod_users.custom_fields WHERE jsonb->>'name' = 'Usergroup';"""

    with patch('builtins.open', mock_open(read_data=sql_content)):
        result = retrieve_usergroup_lookup.function()

    assert result == {
        "opt_1": "sul - borrowdirect brown",
        "opt_2": "sul - university librarian guest",
        "opt_99": "best friend",
    }
    mock_postgres_hook.get_records.assert_called_once()


@patch('libsys_airflow.plugins.folio.reading_room.PostgresHook')
def test_retrieve_usergroup_lookup_empty_result(mock_hook_class):
    """Test usergroup lookup with empty results"""
    mock_hook = MagicMock()
    mock_hook.get_records.return_value = []
    mock_hook_class.return_value = mock_hook

    sql_content = """SELECT jsonb->'selectField'->'options'->'values'
FROM sul_mod_users.custom_fields WHERE jsonb->>'name' = 'Usergroup';"""

    with patch('builtins.open', mock_open(read_data=sql_content)):
        result = retrieve_usergroup_lookup.function()

    # Should return empty dict when no results
    assert result == {}


@patch('libsys_airflow.plugins.folio.reading_room.PostgresHook')
def test_retrieve_usergroup_lookup_connection_error(mock_hook_class, caplog):
    """Test usergroup lookup handles connection errors gracefully"""
    mock_hook = MagicMock()
    mock_hook.get_records.side_effect = Exception("Connection failed")
    mock_hook_class.return_value = mock_hook

    sql_content = """SELECT jsonb->'selectField'->'options'->'values'
FROM sul_mod_users.custom_fields WHERE jsonb->>'name' = 'Usergroup';"""

    with patch('builtins.open', mock_open(read_data=sql_content)):
        result = retrieve_usergroup_lookup.function()

    # Should return empty dict on error
    assert result == {}
    # Should log warning
    assert "Could not retrieve usergroup lookup" in caplog.text


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_patron_group_lookup(mock_client_func):
    """Test patron group lookup retrieval"""
    # Create a mock client instance
    mock_client = MagicMock()
    mock_client.folio_get.return_value = [
        {"id": "b1f10c81-01c1-4b97-9362-6d412df42f52", "group": "faculty"},
        {"id": "c2f10c81-01c1-4b97-9362-6d412df42f53", "group": "courtesy"},
        {"id": "d3f10c81-01c1-4b97-9362-6d412df42f54", "group": "graduate"},
        {"id": "e4f10c81-01c1-4b97-9362-6d412df42f55", "group": "pseudopatron"},
    ]
    mock_client_func.return_value = mock_client

    result = retrieve_patron_group_lookup.function()

    assert result == {
        "b1f10c81-01c1-4b97-9362-6d412df42f52": "faculty",
        "c2f10c81-01c1-4b97-9362-6d412df42f53": "courtesy",
        "d3f10c81-01c1-4b97-9362-6d412df42f54": "graduate",
        "e4f10c81-01c1-4b97-9362-6d412df42f55": "pseudopatron",
    }
    mock_client.folio_get.assert_called_once_with(
        "groups", key="usergroups", query_params={"limit": 99}
    )


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_reading_rooms_lookup(mock_client_func):
    """Test reading rooms lookup retrieval"""
    # Create a mock client instance
    mock_client = MagicMock()
    mock_client.folio_get.return_value = [
        {
            "name": "Green 24-hour study space",
            "id": "95d15527-7bd4-42e7-a629-0618742918e5",
        },
        {"name": "Reading Room A", "id": "a4d15527-7bd4-42e7-a629-0618742918e6"},
    ]
    mock_client_func.return_value = mock_client

    result = retrieve_reading_rooms_lookup.function()

    assert result == {
        "Green 24-hour study space": "95d15527-7bd4-42e7-a629-0618742918e5",
        "Reading Room A": "a4d15527-7bd4-42e7-a629-0618742918e6",
    }
    mock_client.folio_get.assert_called_once_with(
        "reading-room", key="readingRooms", query_params={"limit": 99}
    )


# Test user retrieval
@patch('libsys_airflow.plugins.folio.reading_room.get_current_context')
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_users_batch(mock_client_func, mock_context):
    """Test user batch retrieval"""
    # Create a mock client instance
    mock_client = MagicMock()
    mock_client.folio_get_all.return_value = [
        {
            "id": "user_1",
            "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",
            "customFields": {"usergroup": "opt_1"},
        },
        {
            "id": "user_2",
            "patronGroup": "c2f10c81-01c1-4b97-9362-6d412df42f53",
            "customFields": {"usergroup": "opt_2"},
        },
        {
            "id": "user_3",
            "patronGroup": "d3f10c81-01c1-4b97-9362-6d412df42f54",
            "customFields": {"usergroup": "opt_99"},
        },
    ]
    mock_client_func.return_value = mock_client
    mock_context.return_value = {"params": {"from_date": "2025-12-01"}}

    result = retrieve_users_batch_for_reading_room_access.function()

    assert len(result) == 3
    assert result[0]["id"] == "user_1"
    mock_client.folio_get_all.assert_called_once()


# Test permission generation
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_generate_reading_room_access(
    mock_client_func,
    sample_users,
    lookup_data,
    mock_reading_rooms_config,
):
    """Test reading room access generation"""
    # Create a mock client instance
    mock_client = MagicMock()

    def mock_get(endpoint):
        if endpoint == "reading-room-patron-permission/user_1":
            return [
                {
                    "id": "perm_user_1",
                    "userId": "user_1",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "NOT_ALLOWED",
                    "metadata": {},
                }
            ]
        elif endpoint == "reading-room-patron-permission/user_2":
            return [
                {
                    "id": "perm_user_2",
                    "userId": "user_2",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "ALLOWED",
                    "metadata": {},
                }
            ]
        elif endpoint == "reading-room-patron-permission/user_3":
            return [
                {
                    "id": "perm_user_3",
                    "userId": "user_3",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "NOT_ALLOWED",
                    "metadata": {},
                }
            ]
        elif endpoint == "reading-room-patron-permission/user_4":
            return [
                {
                    "id": "perm_user_4",
                    "userId": "user_4",
                    "readingRoomId": "a4d15527-7bd4-42e7-a629-0618742918e6",
                    "readingRoomName": "Reading Room A",
                    "access": "ALLOWED",
                    "metadata": {},
                }
            ]
        return []

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    result = generate_reading_room_access.function(
        users=sample_users,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check structure
    assert len(result) == 4
    assert all("userId" in item for item in result)
    assert all("permissions" in item for item in result)


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_update_reading_room_permissions(mock_client_func):
    """Test updating reading room permissions in FOLIO"""
    # Create a mock client instance
    mock_client = MagicMock()
    mock_client_func.return_value = mock_client

    # Sample permissions data to update
    reading_room_patron_permissions = [
        {
            "userId": "user_1",
            "permissions": [
                {
                    "id": "perm_1",
                    "userId": "user_1",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "NOT_ALLOWED",
                },
                {
                    "id": "perm_2",
                    "userId": "user_1",
                    "readingRoomId": "a4d15527-7bd4-42e7-a629-0618742918e6",
                    "readingRoomName": "Reading Room A",
                    "access": "ALLOWED",
                },
            ],
        },
        {
            "userId": "user_2",
            "permissions": [
                {
                    "id": "perm_3",
                    "userId": "user_2",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "ALLOWED",
                }
            ],
        },
    ]

    # Execute the function
    update_reading_room_permissions.function(reading_room_patron_permissions)

    # Verify folio_put was called correctly for each user
    assert mock_client.folio_put.call_count == 2

    # Check first call - user_1
    first_call = mock_client.folio_put.call_args_list[0]
    assert first_call[0][0] == "reading-room-patron-permission/user_1"
    assert len(first_call[0][1]) == 2  # Two permissions
    assert first_call[0][1][0]["access"] == "NOT_ALLOWED"
    assert first_call[0][1][1]["access"] == "ALLOWED"

    # Check second call - user_2
    second_call = mock_client.folio_put.call_args_list[1]
    assert second_call[0][0] == "reading-room-patron-permission/user_2"
    assert len(second_call[0][1]) == 1  # One permission
    assert second_call[0][1][0]["access"] == "ALLOWED"


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_update_reading_room_permissions_empty_list(mock_client_func):
    """Test updating with empty permissions list"""
    # Create a mock client instance
    mock_client = MagicMock()
    mock_client_func.return_value = mock_client

    # Execute with empty list
    update_reading_room_permissions.function([])

    # Verify folio_put was never called
    mock_client.folio_put.assert_not_called()


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_update_reading_room_permissions_single_user(mock_client_func):
    """Test updating permissions for a single user"""
    # Create a mock client instance
    mock_client = MagicMock()
    mock_client_func.return_value = mock_client

    reading_room_patron_permissions = [
        {
            "userId": "user_123",
            "permissions": [
                {
                    "id": "perm_abc",
                    "userId": "user_123",
                    "readingRoomId": "room_xyz",
                    "readingRoomName": "Special Collections",
                    "access": "ALLOWED",
                }
            ],
        }
    ]

    update_reading_room_permissions.function(reading_room_patron_permissions)

    # Verify single call
    assert mock_client.folio_put.call_count == 1
    call_args = mock_client.folio_put.call_args
    assert call_args[0][0] == "reading-room-patron-permission/user_123"
    assert call_args[0][1][0]["readingRoomName"] == "Special Collections"


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_update_reading_room_permissions_api_error(mock_client_func):
    """Test handling of API errors during permission updates"""
    # Create a mock client instance with folio_put that raises an exception
    mock_client = MagicMock()
    mock_client.folio_put.side_effect = Exception("FOLIO API error")
    mock_client_func.return_value = mock_client

    reading_room_patron_permissions = [
        {
            "userId": "user_1",
            "permissions": [
                {
                    "id": "perm_1",
                    "userId": "user_1",
                    "readingRoomId": "room_1",
                    "readingRoomName": "Room A",
                    "access": "ALLOWED",
                }
            ],
        }
    ]

    # Should raise the exception (not caught in the function)
    with pytest.raises(Exception, match="FOLIO API error"):
        update_reading_room_permissions.function(reading_room_patron_permissions)

    # Verify it attempted to call folio_put
    mock_client.folio_put.assert_called_once()
