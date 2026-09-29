import json

from typing import Any

import pytest
from unittest.mock import MagicMock, mock_open, patch

from airflow.exceptions import AirflowFailException

from libsys_airflow.plugins.folio.reading_room import (
    retrieve_usergroup_lookup,
    retrieve_patron_group_lookup,
    retrieve_reading_rooms_lookup,
    retrieve_user_id_batches,
    process_user_batch_by_offset,
    summarize_results,
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

    # Mock folio_get to return total count when key="totalRecords"
    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "totalRecords":
            return 250  # Return an integer, not a MagicMock
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client
    mock_context.return_value = {
        "params": {"from_date": "2025-12-01", "user_batch_limit": 100}
    }

    result = retrieve_user_id_batches.function()

    # Result should be a list of batch metadata dictionaries
    assert len(result) == 3  # 250 users / 100 per batch = 3 batches

    # Check first batch
    assert result[0] == {"from_date": "2025-12-01", "offset": 0, "limit": 100}

    # Check second batch
    assert result[1] == {"from_date": "2025-12-01", "offset": 100, "limit": 100}

    # Check third batch (remaining 50 users)
    assert result[2] == {"from_date": "2025-12-01", "offset": 200, "limit": 50}


@patch("libsys_airflow.plugins.folio.reading_room.get_current_context")
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_user_id_batches_no_users(mock_client_func, mock_context):
    """Test retrieving user ID batches when no users found"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "totalRecords":
            return 0  # No users
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client
    mock_context.return_value = {
        "params": {"from_date": "2025-12-01", "user_batch_limit": 100}
    }

    result = retrieve_user_id_batches.function()

    # Should return empty list when no users
    assert result == []


@patch("libsys_airflow.plugins.folio.reading_room.get_current_context")
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_retrieve_user_id_batches_large_dataset(mock_client_func, mock_context):
    """Test retrieving user ID batches with large dataset"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "totalRecords":
            return 130052  # Large number of users
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client
    mock_context.return_value = {
        "params": {"from_date": "2024-11-01", "user_batch_limit": 100}
    }

    result = retrieve_user_id_batches.function()

    # Should create 1301 batches (130052 / 100 = 1300.52 -> 1301)
    assert len(result) == 1301

    # Check first batch
    assert result[0]["offset"] == 0
    assert result[0]["limit"] == 100

    # Check last batch (should have 52 users)
    assert result[-1]["offset"] == 130000
    assert result[-1]["limit"] == 52

    # All batches should have the same from_date
    assert all(batch["from_date"] == "2024-11-01" for batch in result)


# Test process_user_batch_by_offset
@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test processing a batch of users by offset"""
    mock_client = MagicMock()

    # Mock fetching users for this batch
    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            # Return users for this batch
            return [
                {
                    "id": "user_1",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                    "customFields": {"usergroup": "opt_1"},  # sul - borrowdirect brown
                },
                {
                    "id": "user_2",
                    "patronGroup": "c2f10c81-01c1-4b97-9362-6d412df42f53",  # courtesy
                    "customFields": {"usergroup": "opt_99"},  # best friend
                },
                {
                    "id": "user_3",
                    "patronGroup": "d3f10c81-01c1-4b97-9362-6d412df42f54",  # graduate
                    "customFields": {"usergroup": "opt_99"},  # best friend
                },
            ]
        # Existing permissions - user_1 has wrong access (will be updated)
        elif endpoint == "reading-room-patron-permission/user_1":
            return [
                {
                    "id": "perm_user_1",
                    "userId": "user_1",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "ALLOWED",  # Should be NOT_ALLOWED (user has disallowed usergroup)
                    "metadata": {},
                }
            ]
        # user_2 and user_3 have no existing permissions (will add new)
        elif endpoint == "reading-room-patron-permission/user_2":
            return []
        elif endpoint == "reading-room-patron-permission/user_3":
            return []
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary
    assert result["batch_size"] == 3
    assert result["offset"] == 0
    assert result["updates_count"] == 3  # All 3 users need updates
    assert result["skipped_count"] == 0
    assert result["errors_count"] == 0

    # Verify folio_put was called 3 times (once per user)
    assert mock_client.folio_put.call_count == 3


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_with_errors(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test processing a batch with some errors"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return [
                {
                    "id": "user_1",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",
                    "customFields": {"usergroup": "opt_1"},
                },
                {
                    "id": "user_2",
                    "patronGroup": "c2f10c81-01c1-4b97-9362-6d412df42f53",
                    "customFields": {"usergroup": "opt_99"},
                },
                {
                    "id": "user_3",
                    "patronGroup": "d3f10c81-01c1-4b97-9362-6d412df42f54",
                    "customFields": {"usergroup": "opt_99"},
                },
            ]
        elif endpoint == "reading-room-patron-permission/user_1":
            return []
        elif endpoint == "reading-room-patron-permission/user_2":
            # Simulate API error for user_2
            raise Exception("Permission not found")
        elif endpoint == "reading-room-patron-permission/user_3":
            return []
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary
    assert result["batch_size"] == 3
    assert result["updates_count"] == 2  # user_1 and user_3 succeeded
    assert result["skipped_count"] == 0
    assert result["errors_count"] == 1  # user_2 failed

    # Verify folio_put was called 2 times (not for failed user)
    assert mock_client.folio_put.call_count == 2


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_permission_update_failure(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test when permission update fails"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return [
                {
                    "id": "user_1",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",
                    "customFields": {"usergroup": "opt_1"},
                }
            ]
        elif "reading-room-patron-permission" in endpoint:
            return []
        return {}

    mock_client.folio_get.side_effect = mock_get
    # Make folio_put fail
    mock_client.folio_put.side_effect = Exception("Permission update failed")
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01T00:00:00", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary - should count as error
    assert result["batch_size"] == 1
    assert result["updates_count"] == 0
    assert result["skipped_count"] == 0
    assert result["errors_count"] == 1
    assert result["errors"] == [
        {"user_id": "user_1", "error": "Permission update failed"}
    ]


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_empty(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test processing empty batch"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return []
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary
    assert result["batch_size"] == 0
    assert result["updates_count"] == 0
    assert result["skipped_count"] == 0
    assert result["errors_count"] == 0

    # Verify folio_put was not called
    mock_client.folio_put.assert_not_called()


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_no_changes_needed(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test that users with correct permissions are skipped"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return [
                {
                    "id": "user_1",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                    "customFields": {
                        "usergroup": "opt_1"
                    },  # sul - borrowdirect brown (disallowed)
                }
            ]
        elif endpoint == "reading-room-patron-permission/user_1":
            # User already has correct permissions
            return [
                {
                    "id": "perm_user_1",
                    "userId": "user_1",
                    "readingRoomId": "95d15527-7bd4-42e7-a629-0618742918e5",
                    "readingRoomName": "Green 24-hour study space",
                    "access": "NOT_ALLOWED",  # Correct - faculty but disallowed usergroup
                    "metadata": {},
                },
                {
                    "id": "perm_user_2",
                    "userId": "user_1",
                    "readingRoomId": "a4d15527-7bd4-42e7-a629-0618742918e6",
                    "readingRoomName": "Reading Room A",
                    "access": "NOT_ALLOWED",  # Correct - faculty but disallowed usergroup
                    "metadata": {},
                },
            ]
        return {}

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Check result summary - user should be skipped
    assert result["batch_size"] == 1
    assert result["updates_count"] == 0
    assert result["skipped_count"] == 1  # User already has correct permissions
    assert result["errors_count"] == 0

    # Verify folio_put was NOT called
    mock_client.folio_put.assert_not_called()


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_access_determination(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test that access is correctly determined based on patron group and usergroup"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return [
                {
                    "id": "user_faculty_allowed",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                    "customFields": {
                        "usergroup": "opt_99"
                    },  # best friend (not disallowed)
                },
                {
                    "id": "user_faculty_disallowed",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                    "customFields": {
                        "usergroup": "opt_1"
                    },  # sul - borrowdirect brown (disallowed)
                },
                {
                    "id": "user_not_allowed",
                    "patronGroup": "e4f10c81-01c1-4b97-9362-6d412df42f55",  # pseudopatron (not in allowed list)
                    "customFields": {"usergroup": "opt_99"},
                },
            ]
        elif "reading-room-patron-permission" in endpoint:
            return []
        return {}

    permissions_captured = {}

    def mock_put(endpoint, permissions):
        # Capture the permissions being set
        user_id = endpoint.split('/')[-1]
        permissions_captured[user_id] = json.loads(permissions)

    mock_client.folio_get.side_effect = mock_get
    mock_client.folio_put.side_effect = mock_put
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01T00:00:00", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
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
    assert result["skipped_count"] == 0
    assert result["errors_count"] == 0


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_preserves_existing_permissions(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """Test that existing permissions are updated, not replaced"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return [
                {
                    "id": "user_1",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                    "customFields": {"usergroup": "opt_99"},  # best friend (allowed)
                }
            ]
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
        permissions_captured = json.loads(permissions)

    mock_client.folio_get.side_effect = mock_get
    mock_client.folio_put.side_effect = mock_put
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    # Verify existing permission IDs are preserved
    assert any(p.get("id") == "existing_perm_1" for p in permissions_captured)
    assert any(p.get("id") == "existing_perm_2" for p in permissions_captured)

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
    assert result["skipped_count"] == 0
    assert result["errors_count"] == 0


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_skips_missing_rooms(
    mock_client_func, lookup_data, mock_reading_rooms_config, caplog
):
    """Test that rooms in config but not in FOLIO are skipped"""
    mock_client = MagicMock()

    # Create a reading_rooms lookup that's missing "Reading Room A"
    incomplete_reading_rooms = {
        "Green 24-hour study space": "95d15527-7bd4-42e7-a629-0618742918e5",
        # "Reading Room A" is missing
    }

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return [
                {
                    "id": "user_1",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",  # faculty
                    "customFields": {"usergroup": "opt_99"},
                }
            ]
        elif "reading-room-patron-permission" in endpoint:
            return []
        return {}

    permissions_captured = None

    def mock_put(endpoint, permissions):
        nonlocal permissions_captured
        permissions_captured = json.loads(permissions)

    mock_client.folio_get.side_effect = mock_get
    mock_client.folio_put.side_effect = mock_put
    mock_client_func.return_value = mock_client

    batch_metadata = {"from_date": "2024-11-01", "offset": 0, "limit": 100}

    result = process_user_batch_by_offset.function(
        batch_metadata=batch_metadata,
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=incomplete_reading_rooms,  # Missing "Reading Room A"
    )

    # Should only create permission for "Green 24-hour study space"
    assert len(permissions_captured) == 1
    assert permissions_captured[0]["readingRoomName"] == "Green 24-hour study space"

    # Should log warning about missing room (only on first batch, offset=0)
    assert "Rooms in config but not in FOLIO" in caplog.text
    assert "Reading Room A" in caplog.text

    assert result["updates_count"] == 1
    assert result["errors_count"] == 0


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_puts_json_string(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    """FolioClient rejects list payloads, so permissions are sent as a JSON string"""
    mock_client = MagicMock()

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            return [
                {
                    "id": "user_1",
                    "patronGroup": "b1f10c81-01c1-4b97-9362-6d412df42f52",
                    "customFields": {"usergroup": "opt_99"},
                }
            ]
        return []

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    process_user_batch_by_offset.function(
        batch_metadata={"from_date": "2024-11-01", "offset": 0, "limit": 100},
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    path, payload = mock_client.folio_put.call_args.args
    assert path == "reading-room-patron-permission/user_1"
    assert isinstance(payload, str)
    assert len(json.loads(payload)) == 2


@pytest.fixture
def mock_dag_run():
    dag_run = MagicMock()
    dag_run.run_id = "scheduled__2026-09-24T10:30:00+00:00"
    dag_run.dag_id = "reading_room_access"
    return dag_run


LOOKUPS: dict[str, Any] = {"usergroups": {}, "patron_groups": {}, "reading_rooms": {}}


def _batch_result(offset, updates=0, skipped=0, errors=None, errors_count=None):
    errors = errors or []
    errors_count = len(errors) if errors_count is None else errors_count
    return {
        "batch_size": updates + skipped + errors_count,
        "offset": offset,
        "updates_count": updates,
        "skipped_count": skipped,
        "errors_count": errors_count,
        "errors": errors,
    }


@patch("libsys_airflow.plugins.folio.reading_room.send_email_with_server_name")
def test_summarize_results_no_errors(mock_send_email, mock_dag_run):
    batch_metadata = [{"offset": 0}, {"offset": 500}]
    batch_results = [
        _batch_result(0, updates=2, skipped=498),
        _batch_result(500, updates=1, skipped=9),
    ]

    summary = summarize_results.function(
        batch_metadata, batch_results, dag_run=mock_dag_run, **LOOKUPS
    )

    assert summary == {
        "users_count": 510,
        "updates_count": 3,
        "skipped_count": 507,
        "errors_count": 0,
        "failed_batches": 0,
    }
    mock_send_email.assert_not_called()


@patch("libsys_airflow.plugins.folio.reading_room.send_email_with_server_name")
def test_summarize_results_no_batches(mock_send_email, mock_dag_run):
    summary = summarize_results.function([], [], dag_run=mock_dag_run, **LOOKUPS)

    assert summary["users_count"] == 0
    mock_send_email.assert_not_called()


@patch(
    "libsys_airflow.plugins.shared.utils.airflow_url",
    return_value="https://airflow.example.edu/",
)
@patch("libsys_airflow.plugins.folio.reading_room.send_email_with_server_name")
def test_summarize_results_user_errors(mock_send_email, mock_airflow_url, mock_dag_run):
    batch_metadata = [{"offset": 0}, {"offset": 500}]
    batch_results = [
        _batch_result(
            0,
            skipped=499,
            errors=[{"user_id": "user_1", "error": "Payload must be a dictionary"}],
        ),
        _batch_result(500, updates=10),
    ]

    with pytest.raises(
        AirflowFailException,
        match="0 failed tasks, 0 unprocessed batches, 1 user error$",
    ):
        summarize_results.function(
            batch_metadata, batch_results, dag_run=mock_dag_run, **LOOKUPS
        )

    mock_send_email.assert_called_once()
    kwargs = mock_send_email.call_args.kwargs
    assert kwargs["subject"] == "Reading Room Access Failures"
    html = kwargs["html_content"]
    assert (
        'href="https://airflow.example.edu/dags/reading_room_access/runs/'
        'scheduled__2026-09-24T10%3A30%3A00%2B00%3A00"'
    ) in html
    assert "Users fetched: 510" in html
    assert "Updated: 10" in html
    assert "Skipped (no changes): 499" in html
    assert "Errors: 1" in html
    assert "user_1: Payload must be a dictionary" in html
    assert "Batches not processed" not in html
    assert "Failed tasks" not in html


@patch(
    "libsys_airflow.plugins.shared.utils.airflow_url",
    return_value="https://airflow.example.edu/",
)
@patch("libsys_airflow.plugins.folio.reading_room.send_email_with_server_name")
def test_summarize_results_failed_batch(
    mock_send_email, mock_airflow_url, mock_dag_run
):
    batch_metadata = [{"offset": 0}, {"offset": 500}]
    batch_results = [_batch_result(0, updates=5)]

    with pytest.raises(AirflowFailException, match="1 unprocessed batch,"):
        summarize_results.function(
            batch_metadata, batch_results, dag_run=mock_dag_run, **LOOKUPS
        )

    html = mock_send_email.call_args.kwargs["html_content"]
    assert "Batches not processed: 1" in html


@patch(
    "libsys_airflow.plugins.shared.utils.airflow_url",
    return_value="https://airflow.example.edu/",
)
@patch("libsys_airflow.plugins.folio.reading_room.send_email_with_server_name")
def test_summarize_results_batch_retrieval_failed(
    mock_send_email, mock_airflow_url, mock_dag_run
):
    """A failed retrieve_user_id_batches resolves to None and must fail the run"""
    with pytest.raises(AirflowFailException, match="^1 failed task,"):
        summarize_results.function(None, [], dag_run=mock_dag_run, **LOOKUPS)

    html = mock_send_email.call_args.kwargs["html_content"]
    assert "Failed tasks: retrieve_user_id_batches" in html


@patch(
    "libsys_airflow.plugins.shared.utils.airflow_url",
    return_value="https://airflow.example.edu/",
)
@patch("libsys_airflow.plugins.folio.reading_room.send_email_with_server_name")
def test_summarize_results_lookup_failed_with_no_users(
    mock_send_email, mock_airflow_url, mock_dag_run
):
    """A failed lookup fails the run even when there were no batches to process"""
    lookups: dict[str, Any] = {**LOOKUPS, "patron_groups": None}

    with pytest.raises(AirflowFailException, match="^1 failed task,"):
        summarize_results.function([], [], dag_run=mock_dag_run, **lookups)

    html = mock_send_email.call_args.kwargs["html_content"]
    assert "Failed tasks: retrieve_patron_group_lookup" in html


@patch(
    "libsys_airflow.plugins.shared.utils.airflow_url",
    return_value="https://airflow.example.edu/",
)
@patch("libsys_airflow.plugins.folio.reading_room.send_email_with_server_name")
def test_summarize_results_escapes_errors_and_counts_truncated(
    mock_send_email, mock_airflow_url, mock_dag_run
):
    batch_results = [
        _batch_result(
            0,
            errors=[{"user_id": "user_1", "error": "<html>Bad Gateway</html>"}],
            errors_count=250,
        )
    ]

    with pytest.raises(AirflowFailException, match="250 user errors$"):
        summarize_results.function(
            [{"offset": 0}], batch_results, dag_run=mock_dag_run, **LOOKUPS
        )

    html = mock_send_email.call_args.kwargs["html_content"]
    assert "&lt;html&gt;Bad Gateway&lt;/html&gt;" in html
    assert "Errors: 250" in html
    assert "...and 249 more" in html


@patch("libsys_airflow.plugins.folio.reading_room.folio_client")
def test_process_user_batch_by_offset_caps_errors_and_sorts(
    mock_client_func, lookup_data, mock_reading_rooms_config
):
    mock_client = MagicMock()
    users = [
        {"id": f"user_{i}", "patronGroup": "", "customFields": {}} for i in range(150)
    ]
    query_params_seen = []

    def mock_get(endpoint, key=None, query_params=None):
        if endpoint == "users" and key == "users":
            query_params_seen.append(query_params)
            return users
        raise Exception("FOLIO is down")

    mock_client.folio_get.side_effect = mock_get
    mock_client_func.return_value = mock_client

    result = process_user_batch_by_offset.function(
        batch_metadata={"from_date": "2024-11-01", "offset": 0, "limit": 150},
        usergroups=lookup_data["usergroups"],
        patron_groups=lookup_data["patron_groups"],
        reading_rooms=lookup_data["reading_rooms"],
    )

    assert query_params_seen[0]["query"] == 'updatedDate>"2024-11-01" sortBy id'
    assert result["errors_count"] == 150
    assert len(result["errors"]) == 100
