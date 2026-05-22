import logging
import uuid

from attrs import define
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

from airflow.sdk import task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.shared.folio_client import folio_client
from libsys_airflow.plugins.folio.helpers.constants import reading_rooms_config

logger = logging.getLogger(__name__)


def get_usergroup_sql_path(**kwargs) -> str:
    """Get path to usergroups SQL file"""
    sql_path = (
        Path(kwargs.get("airflow", "/opt/airflow"))
        / "libsys_airflow/plugins/folio/helpers/usergroups.sql"
    )
    return str(sql_path)


@task
def retrieve_usergroup_lookup() -> dict:
    """Retrieve usergroup custom field options from FOLIO database"""
    lookup = {}
    sql_path = get_usergroup_sql_path()

    logger.info("Retrieving usergroup custom field options")

    try:
        with open(sql_path) as sqv:
            query = sqv.read()

        logger.info(f"Executing query from {sql_path}")

        pg_hook = PostgresHook(postgres_conn_id="postgres_folio")
        results = pg_hook.get_records(query)

        if results and results[0] and results[0][0]:
            for opt in results[0][0]:
                lookup[opt['id']] = opt['value']

        logger.info(f"Retrieved {len(lookup)} usergroup options")

    except Exception as e:
        logger.warning(
            f"Could not retrieve usergroup lookup from postgres_folio connection: {e}"
        )
        # Return empty lookup if connection doesn't exist or fails

    return lookup


@task
def retrieve_patron_group_lookup() -> dict:
    """Retrieve patron groups from FOLIO"""
    lookup = {}
    client = folio_client()

    logger.info("Retrieving patron groups")
    groups = client.folio_get("groups", key="usergroups", query_params={"limit": 99})

    for g in groups:
        lookup[g['id']] = g['group']

    logger.info(f"Retrieved {len(lookup)} patron groups")
    return lookup


@task
def retrieve_reading_rooms_lookup() -> dict:
    """Retrieve reading rooms from FOLIO"""
    lookup = {}
    client = folio_client()

    logger.info("Retrieving reading rooms")
    rooms = client.folio_get(
        "reading-room", key="readingRooms", query_params={"limit": 99}
    )

    for r in rooms:
        lookup[r['name']] = r['id']

    logger.info(f"Retrieved {len(lookup)} reading rooms")
    return lookup


@define
class FolioUser:
    id: str
    patronGroup: str
    customFields: dict
    patron_groups: dict
    usergroups: dict

    @property
    def patron_group_name(self) -> str:
        return self.patron_groups.get(self.patronGroup, "")

    @property
    def usergroup_name(self) -> str:
        return self.usergroups.get(self.customFields.get("usergroup", ""), "")


def formatted_date(from_date: Union[str, None]) -> str:
    """Format date for FOLIO query"""
    if not from_date:
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()
    else:
        formatted_date = from_date

    return formatted_date


@task
def retrieve_users_batch_for_reading_room_access() -> list:
    """Retrieve users with reading room access from FOLIO and return in batches"""
    context = get_current_context()
    params = context.get("params", {})
    from_date = params.get("from_date")
    user_batch_limit = params.get("user_batch_limit", 500)

    client = folio_client()
    query_date = formatted_date(from_date)
    logger.info(f"Retrieving users updated after {formatted_date(from_date)}")

    # folio_get_all returns a generator
    users_generator = client.folio_get_all(
        "users",
        key="users",
        query=f'updatedDate>"{query_date}"',
        limit=1000,  # Fetch 1000 users per API call
    )

    # Convert generator to list
    all_users = [row for row in users_generator]
    logger.info(f"Retrieved {len(all_users)} users")

    # Split into batches
    if len(all_users) == 0:
        logger.info("No users to process")
        return []

    # Create batches using user_batch_limit
    user_batches = [
        all_users[x : x + user_batch_limit]
        for x in range(0, len(all_users), user_batch_limit)
    ]

    logger.info(f"Split into {len(user_batches)} batches")
    return user_batches


@task(max_active_tis_per_dag=5)
def generate_reading_room_access_batch(
    user_batch: list,
    usergroups: dict,
    patron_groups: dict,
    reading_rooms: dict,
) -> list:
    """Generate reading room access data for a batch of users in FOLIO"""
    reading_room_patron_permissions = []

    logger.info(f"Generating reading room access for batch of {len(user_batch)} users")
    client = folio_client()

    for user in user_batch:
        folio_user = FolioUser(
            id=user["id"],
            patronGroup=user.get("patronGroup", ""),
            customFields=user.get("customFields", {}),
            patron_groups=patron_groups,
            usergroups=usergroups,
        )

        permissions = []
        existing_access = []

        # Get existing permissions
        try:
            existing_perms = client.folio_get(
                f"reading-room-patron-permission/{folio_user.id}"
            )

            for existing_perm in existing_perms:
                perm_id = existing_perm.get("id", str(uuid.uuid4()))
                existing_perm["id"] = perm_id
                if "metadata" in existing_perm:
                    del existing_perm["metadata"]
                existing_access.append(existing_perm)
        except Exception as e:
            logger.warning(
                f"Could not retrieve existing permissions for user {folio_user.id}: {e}"
            )

        # Determine access for each reading room
        for room_name, room_config in reading_rooms_config.items():
            access = "NOT_ALLOWED"

            # Check if allowed based on patron group
            for allowed_list in room_config['allowed'].values():
                if folio_user.patron_group_name in allowed_list:
                    access = "ALLOWED"
                    break

            # Check if disallowed based on usergroup (overrides allowed)
            for disallowed_list in room_config['disallowed'].values():
                if folio_user.usergroup_name in disallowed_list:
                    access = "NOT_ALLOWED"
                    break

            """
            All users should have an existing entry for each reading room when the
            reading room is created in FOLIO. However, to be safe, we check here and
            only add a new entry if one does not already exist.
            """
            updated = False
            for ea in existing_access:
                if ea.get('readingRoomName') == room_name:
                    ea['access'] = access
                    updated = True
                    break

            # Add new entry if none exists
            if not updated and not any(
                ea.get('readingRoomName') == room_name for ea in existing_access
            ):
                permissions.append(
                    {
                        "id": str(uuid.uuid4()),
                        "userId": folio_user.id,
                        "readingRoomId": reading_rooms.get(room_name),
                        "readingRoomName": room_name,
                        "access": access,
                    }
                )

        # Combine new and existing permissions
        permissions.extend(existing_access)

        patron_permissions = {"userId": folio_user.id, "permissions": permissions}
        reading_room_patron_permissions.append(patron_permissions)

    logger.info(
        f"Generated permissions for {len(reading_room_patron_permissions)} users in batch"
    )
    return reading_room_patron_permissions


@task(max_active_tis_per_dag=5)
def update_reading_room_permissions_batch(reading_room_patron_permissions: list):
    """Update reading room access permissions in FOLIO for a batch"""
    client = folio_client()

    logger.info(
        f"Updating reading room patron permissions for batch of {len(reading_room_patron_permissions)} users"
    )

    success_count = 0
    error_count = 0

    for patron_permission in reading_room_patron_permissions:
        user_id = patron_permission['userId']
        permissions = patron_permission['permissions']
        try:
            client.folio_put(
                f"reading-room-patron-permission/{user_id}",
                permissions,
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to update permissions for user {user_id}: {e}")
            error_count += 1

    logger.info(f"Completed batch: {success_count} successful, {error_count} failed")

    if error_count > 0:
        raise Exception(
            f"Failed to update permissions for {error_count} users in batch"
        )
