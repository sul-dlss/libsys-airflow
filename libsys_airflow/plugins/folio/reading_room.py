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
    """Retrieve usergroup custom field options from PostgreSQL"""
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
def retrieve_user_id_batches() -> list:
    """Retrieve count of users and return batch metadata (start/end indices)"""
    context = get_current_context()
    params = context.get("params", {})
    from_date = params.get("from_date")
    user_batch_limit = params.get("user_batch_limit", 500)

    client = folio_client()
    query_date = formatted_date(from_date)
    logger.info(f"Retrieving user count for users updated after {query_date}")
    logger.info(f"Using batch size: {user_batch_limit}")

    # Get total count of users without fetching all data
    query = f'updatedDate>"{query_date}"'
    total_records = client.folio_get(
        "users",
        key="totalRecords",
        query_params={
            "query": query,
            "limit": 1,  # We only need the count, not the data
            "offset": 0,
        },
    )

    logger.info(f"Total users to process: {total_records}")

    if total_records == 0:
        logger.info("No users to process")
        return []

    # Create batch metadata (offset and limit for each batch)
    batch_metadata = []
    for offset in range(0, total_records, user_batch_limit):
        batch_metadata.append(
            {
                "from_date": query_date,
                "offset": offset,
                "limit": min(user_batch_limit, total_records - offset),
            }
        )

    logger.info(f"Created {len(batch_metadata)} batch metadata entries")
    return batch_metadata


@task(max_active_tis_per_dag=5)
def process_user_batch_by_offset(
    batch_metadata: dict,
    usergroups: dict,
    patron_groups: dict,
    reading_rooms: dict,
) -> dict:
    """Fetch users for this batch and process their permissions"""
    offset = batch_metadata["offset"]
    limit = batch_metadata["limit"]
    from_date = batch_metadata["from_date"]

    logger.info(f"Processing batch: offset={offset}, limit={limit}")

    client = folio_client()

    # Fetch users for this batch
    query = f'updatedDate>"{from_date}"'
    users = client.folio_get(
        "users",
        key="users",
        query_params={"query": query, "limit": limit, "offset": offset},
    )

    logger.info(f"Fetched {len(users)} users for this batch")

    # Process these users
    updates_count = 0
    skipped_count = 0
    errors_count = 0

    # Warn about config/FOLIO room mismatches (only log once on first batch)
    if offset == 0:
        config_rooms = set(reading_rooms_config.keys())
        folio_rooms = set(reading_rooms.keys())
        missing_in_folio = config_rooms - folio_rooms
        missing_in_config = folio_rooms - config_rooms

        if missing_in_folio:
            logger.warning(
                f"Rooms in config but not in FOLIO: {missing_in_folio}. "
                f"These rooms will be skipped."
            )
        if missing_in_config:
            logger.info(
                f"Rooms in FOLIO but not in config: {missing_in_config}. "
                f"Existing permissions for these rooms will be preserved as-is."
            )

    for user in users:
        try:
            folio_user = FolioUser(
                id=user["id"],
                patronGroup=user.get("patronGroup", ""),
                customFields=user.get("customFields", {}),
                patron_groups=patron_groups,
                usergroups=usergroups,
            )

            new_permissions = []
            existing_access = []
            has_changes = False

            # Get existing permissions
            try:
                existing_perms = client.folio_get(
                    f"reading-room-patron-permission/{folio_user.id}"
                )

                for existing_perm in existing_perms:
                    # Ensure id exists, generate if missing
                    if 'id' not in existing_perm:
                        existing_perm['id'] = str(uuid.uuid4())
                    # Remove metadata
                    if 'metadata' in existing_perm:
                        del existing_perm['metadata']
                    existing_access.append(existing_perm)

            except Exception as e:
                logger.warning(
                    f"Could not retrieve existing permissions for user {folio_user.id}: {e}"
                )
                errors_count += 1
                continue  # Skip this user

            # Determine access for each reading room in our config
            for room_name, room_config in reading_rooms_config.items():
                # Skip if room doesn't exist in FOLIO
                room_id = reading_rooms.get(room_name)
                if room_id is None:
                    continue

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

                # Check if user already has this permission with the correct access
                found_existing = False
                for ea in existing_access:
                    if ea.get('readingRoomName') == room_name:
                        found_existing = True
                        # Check if access needs to be updated
                        if ea.get('access') != access:
                            ea['access'] = access
                            has_changes = True
                        break

                """
                All users should have an existing entry for each reading room when the
                reading room is created in FOLIO. However, to be safe, we check here and
                only add a new entry if one does not already exist.
                """
                if not found_existing:
                    # Include 'id' for new permissions - REQUIRED by API
                    new_permissions.append(
                        {
                            "id": str(uuid.uuid4()),
                            "userId": folio_user.id,
                            "readingRoomId": room_id,
                            "readingRoomName": room_name,
                            "access": access,
                        }
                    )
                    has_changes = True

            # Combine new and existing permissions
            all_permissions = new_permissions + existing_access

            # Only do PUT if there are changes
            if not has_changes:
                skipped_count += 1
                continue

            # Validate all permissions before sending
            for perm in all_permissions:
                if not perm.get('id') or not perm.get('readingRoomId'):
                    raise ValueError(
                        f"Invalid permission structure for user {folio_user.id}"
                    )

            # Update permissions in FOLIO
            client.folio_put(
                f"reading-room-patron-permission/{folio_user.id}",
                all_permissions,
            )
            updates_count += 1

        except Exception as e:
            logger.error(f"Failed to process user {user['id']}: {e}")
            errors_count += 1

    logger.info(
        f"Completed batch (offset={offset}): {updates_count} updated, "
        f"{skipped_count} skipped, {errors_count} errors"
    )

    return {
        "batch_size": len(users),
        "offset": offset,
        "updates_count": updates_count,
        "skipped_count": skipped_count,
        "errors_count": errors_count,
    }
