import pytest
from unittest.mock import MagicMock, mock_open, patch

from libsys_airflow.plugins.folio.reading_room import (
    retrieve_usergroup_lookup,
    retrieve_patron_group_lookup,
    retrieve_reading_rooms_lookup,
    retrieve_user_id_batches,
    process_user_id_batch,
    formatted_date,
    get_usergroup_sql_path,
)


@pytest.fixture
def mock_postgres_hook():
    """Mock PostgresHook for usergroup lookup"""
    mock_hook = MagicMock()
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
    from datetime import datetime

    # Mock datetime.now() to return a specific date
    mock_now = datetime(2026, 5, 8, 15, 30, 45, 123456)
    mock_datetime.now.return_value = mock_now
    # Need to keep the real datetime class for timedelta operations
    mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

    result = formatted_date(None)

    # Should be yesterday (May 7) at midnight in ISO format
    assert result == "2026-05-07T00:00:00"


# Test lookup tasks
@patch("libsys_airflow.plugins.folio.reading_room.PostgresHook")
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


@patch("libsys_airflow.plugins.folio.reading_room.PostgresHook")
def test_retrieve_usergroup_lookup_empty_result(mock_hook_class):
    """Test usergroup lookup with empty results"""
    mock_hook = MagicMock()
    mock_hook.get_records.return_value = []
    mock_hook_class.return_value = mock_hook

    sql_content = """SELECT jsonb->'selectField'->'options'->'values'
FROM sul_mod_users.custom_fields WHERE jsonb->>'name' = 'Usergroup';"""

    with patch("builtins.open", mock_open(read_data=sql_content)):
        result = retrieve_usergroup_lookup.function()

    assert result == {}


@patch("libsys_airflow.plugins.folio.reading_room.PostgresHook")
def test_retrieve_usergroup_lookup_connection_error(mock_hook_class, caplog):
    """Test usergroup lookup handles connection errors gracefully"""
    mock_hook = MagicMock()
    mock_hook.get_records.side_effect = Exception("Connection failed")
    mock_hook_class.return_value = mock_hook

    sql_content = """SELECT jsonb->'selectField'->'options'->'values'
FROM sul_mod_users.custom_fields WHERE jsonb->>'name' = 'Usergroup';"""

    with patch("builtins.open", mock_open(read_data=sql_content)):
        result = retrieve_usergroup_lookup.function()

    assert result == {}
    assert "Could not retrieve usergroup lookup" in caplog.text


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_patron_group_lookup(mock_client_func):
    """Test patron group lookup retrieval"""
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


# Test user ID batch retrieval
@patch("libsys_airflow.plugins.folio.reading_room.get_current_context")
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_user_id_batches(mock_client_func, mock_context):
    """Test user ID batch retrieval"""
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
    mock_context.return_value = {
        "params": {"from_date": "2025-12-01", "user_batch_limit": 100}
    }

    result = retrieve_user_id_batches.function()

    # Result should be a list of batches containing only user IDs
    assert len(result) == 1  # 1 batch (3 users with batch_limit of 100)
    assert len(result[0]) == 3  # First batch contains 3 user IDs
    assert result[0] == ["user_1", "user_2", "user_3"]  # Just IDs, not full objects
    mock_client.folio_get_all.assert_called_once()


@patch("libsys_airflow.plugins.folio.reading_room.get_current_context")
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_user_id_batches_large_dataset(mock_client_func, mock_context):
    """Test user ID batch retrieval with large dataset"""
    mock_client = MagicMock()

    # Create 250 mock users
    mock_users = [
        {
            "id": f"user_{i}",
            "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",
            "customFields": {"usergroup": "opt_1"},
        }
        for i in range(250)
    ]

    mock_client.folio_get_all.return_value = mock_users
    mock_client_func.return_value = mock_client
    mock_context.return_value = {
        "params": {"from_date": "2025-12-01", "user_batch_limit": 100}
    }

    result = retrieve_user_id_batches.function()

    # Result should be 3 batches (100, 100, 50)
    assert len(result) == 3
    assert len(result[0]) == 100  # First batch
    assert len(result[1]) == 100  # Second batch
    assert len(result[2]) == 50  # Third batch (remaining users)
    # Verify they're just IDs
    assert result[0][0] == "user_0"
    assert result[2][-1] == "user_249"


@patch("libsys_airflow.plugins.folio.reading_room.get_current_context")
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_user_id_batches_empty_result(mock_client_func, mock_context):
    """Test user ID batch retrieval with no users"""
    mock_client = MagicMock()
    mock_client.folio_get_all.return_value = []
    mock_client_func.return_value = mock_client
    mock_context.return_value = {
        "params": {"from_date": "2025-12-01", "user_batch_limit": 100}
    }

    result = retrieve_user_id_batches.function()

    assert result == []
    mock_client.folio_get_all.assert_called_once()


# Test process_user_id_batch
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_id_batch(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test processing a batch of user IDs"""
    mock_client = MagicMock()

    # Mock user data retrieval
    def mock_get(endpoint):
        if endpoint == "users/user_1":
            return {
                "id": "user_1",
                "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                "customFields": {"usergroup": "opt_1"},  # sul - borrowdirect brown
            }
        elif endpoint == "users/user_2":
            return {
                "id": "user_2",
                "patronGroup": "c2f10c81-01c1-4b97-9362-6d412df42f53",  # courtesy
                "customFields": {"usergroup": "opt_99"},  # best friend
            }
        elif endpoint == "users/user_3":
            return {
                "id": "user_3",
                "patronGroup": "d3f10c81-01c1-4b97-9362-6d412df42f54",  # graduate
                "customFields": {"usergroup": "opt_99"},  # best friend
            }
        # Existing permissions
        elif endpoint == "reading-room-patron-permission/user_1":
            return [
                {
                    "id": "perm_user_1",
                    "userId": "user_1",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "ALLOWED",
                    "metadata": {},
                }
            ]
        elif endpoint == "reading-room-patron-permission/user_2":
            return []
        elif endpoint == "reading-room-patron-permission/user_3":
            return []
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    user_id_batch = ["user_1", "user_2", "user_3"]

    result = process_user_id_batch.function(
        user_id_batch=user_id_batch,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary
    assert result["batch_size"] == 3
    assert result["updates_count"] == 3
    assert result["errors_count"] == 0

    # Verify folio_put was called 3 times (once per user)
    assert mock_client.folio_put.call_count == 3


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_id_batch_with_errors(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test processing a batch with some errors"""
    mock_client = MagicMock()

    call_count = 0

    def mock_get(endpoint):
        nonlocal call_count
        if endpoint == "users/user_1":
            return {
                "id": "user_1",
                "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",
                "customFields": {"usergroup": "opt_1"},
            }
        elif endpoint == "users/user_2":
            # Simulate API error for user_2
            raise Exception("User not found")
        elif endpoint == "users/user_3":
            return {
                "id": "user_3",
                "patronGroup": "d3f10c81-01c1-4b97-9362-6d412df42f54",
                "customFields": {"usergroup": "opt_99"},
            }
        elif "reading-room-patron-permission" in endpoint:
            return []
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    user_id_batch = ["user_1", "user_2", "user_3"]

    result = process_user_id_batch.function(
        user_id_batch=user_id_batch,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary
    assert result["batch_size"] == 3
    assert result["updates_count"] == 2  # user_1 and user_3 succeeded
    assert result["errors_count"] == 1  # user_2 failed

    # Verify folio_put was called 2 times (not for failed user)
    assert mock_client.folio_put.call_count == 2


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_id_batch_permission_update_failure(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test when permission update fails"""
    mock_client = MagicMock()

    def mock_get(endpoint):
        if endpoint == "users/user_1":
            return {
                "id": "user_1",
                "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",
                "customFields": {"usergroup": "opt_1"},
            }
        elif "reading-room-patron-permission" in endpoint:
            return []
        return {}

    mock_client.folio_get.side_effect = mock_get
    # Make folio_put fail
    mock_client.folio_put.side_effect = Exception("Permission update failed")
    mock_client_func.return_value = mock_client

    user_id_batch = ["user_1"]

    result = process_user_id_batch.function(
        user_id_batch=user_id_batch,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary - should count as error
    assert result["batch_size"] == 1
    assert result["updates_count"] == 0
    assert result["errors_count"] == 1


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_id_batch_empty(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test processing empty batch"""
    mock_client = MagicMock()
    mock_client_func.return_value = mock_client

    user_id_batch = []

    result = process_user_id_batch.function(
        user_id_batch=user_id_batch,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary
    assert result["batch_size"] == 0
    assert result["updates_count"] == 0
    assert result["errors_count"] == 0

    # Verify no API calls were made
    mock_client.folio_get.assert_not_called()
    mock_client.folio_put.assert_not_called()


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_id_batch_access_determination(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test that access is correctly determined based on patron group and usergroup"""
    mock_client = MagicMock()

    def mock_get(endpoint):
        if endpoint == "users/user_faculty_allowed":
            return {
                "id": "user_faculty_allowed",
                "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                "customFields": {"usergroup": "opt_99"},  # best friend (not disallowed)
            }
        elif endpoint == "users/user_faculty_disallowed":
            return {
                "id": "user_faculty_disallowed",
                "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                "customFields": {
                    "usergroup": "opt_1"
                },  # sul - borrowdirect brown (disallowed)
            }
        elif endpoint == "users/user_not_allowed":
            return {
                "id": "user_not_allowed",
                "patronGroup": "e4f10c81-01c1-4b97-9362-6d412df42f55",  # pseudopatron (not in allowed list)
                "customFields": {"usergroup": "opt_99"},
            }
        elif "reading-room-patron-permission" in endpoint:
            return []
        return {}

    permissions_captured = {}

    def mock_put(endpoint, permissions):
        # Capture the permissions being set
        user_id = endpoint.split('/')[-1]
        permissions_captured[user_id] = permissions

    mock_client.folio_get.side_effect = mock_get
    mock_client.folio_put.side_effect = mock_put
    mock_client_func.return_value = mock_client

    user_id_batch = [
        "user_faculty_allowed",
        "user_faculty_disallowed",
        "user_not_allowed",
    ]

    result = process_user_id_batch.function(
        user_id_batch=user_id_batch,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Verify access levels
    # Faculty with allowed usergroup -> ALLOWED
    green_perm_allowed = next(
        p
        for p in permissions_captured["user_faculty_allowed"]
        if p["readingRoomName"] == "Green 24-hour study space"
    )
    assert green_perm_allowed["access"] == "ALLOWED"

    # Faculty with disallowed usergroup -> NOT_ALLOWED (disallow overrides allow)
    green_perm_disallowed = next(
        p
        for p in permissions_captured["user_faculty_disallowed"]
        if p["readingRoomName"] == "Green 24-hour study space"
    )
    assert green_perm_disallowed["access"] == "NOT_ALLOWED"

    # Pseudopatron (not in allowed list) -> NOT_ALLOWED
    green_perm_not_allowed = next(
        p
        for p in permissions_captured["user_not_allowed"]
        if p["readingRoomName"] == "Green 24-hour study space"
    )
    assert green_perm_not_allowed["access"] == "NOT_ALLOWED"

    assert result["updates_count"] == 3
    assert result["errors_count"] == 0


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_id_batch_preserves_existing_permissions(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test that existing permissions are updated, not replaced"""
    mock_client = MagicMock()

    def mock_get(endpoint):
        if endpoint == "users/user_1":
            return {
                "id": "user_1",
                "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                "customFields": {"usergroup": "opt_99"},
            }
        elif endpoint == "reading-room-patron-permission/user_1":
            # User already has existing permissions
            return [
                {
                    "id": "existing_perm_1",
                    "userId": "user_1",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "NOT_ALLOWED",  # Will be updated to ALLOWED
                    "metadata": {"created": "2026-01-01"},
                },
                {
                    "id": "existing_perm_2",
                    "userId": "user_1",
                    "readingRoomId": "a4d15527-7bd4-42e7-a629-0618742918e6",
                    "readingRoomName": "Reading Room A",
                    "access": "ALLOWED",
                    "metadata": {"created": "2026-01-01"},
                },
            ]
        return {}

    permissions_captured = None

    def mock_put(endpoint, permissions):
        nonlocal permissions_captured
        permissions_captured = permissions

    mock_client.folio_get.side_effect = mock_get
    mock_client.folio_put.side_effect = mock_put
    mock_client_func.return_value = mock_client

    user_id_batch = ["user_1"]

    result = process_user_id_batch.function(
        user_id_batch=user_id_batch,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Verify existing permission IDs are preserved
    assert any(p["id"] == "existing_perm_1" for p in permissions_captured)
    assert any(p["id"] == "existing_perm_2" for p in permissions_captured)

    # Verify metadata was removed
    for p in permissions_captured:
        assert "metadata" not in p

    # Verify access was updated based on current rules
    green_perm = next(
        p
        for p in permissions_captured
        if p["readingRoomName"] == "Green 24-hour study space"
    )
    assert green_perm["access"] == "ALLOWED"  # Updated from NOT_ALLOWED
    assert green_perm["id"] == "existing_perm_1"  # Same ID preserved

    assert result["updates_count"] == 1
    assert result["errors_count"] == 0
