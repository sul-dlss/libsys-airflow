import logging
import psycopg2
import uuid

from attrs import define
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

from airflow.decorators import task
from airflow.models import Connection
from airflow.operators.python import get_current_context

from libsys_airflow.plugins.shared.folio_client import folio_client
from libsys_airflow.plugins.folio.helpers.constants import reading_rooms_config

logger = logging.getLogger(__name__)


class ReadingRoomsData:
    def __init__(self):
        self.folio_client = folio_client()
        self.patron_groups = self.patron_group_lookup()
        self.usergroups = self.usergroup_lookup()
        self.reading_rooms = self.reading_rooms_lookup()

    def patron_group_lookup(self) -> dict:
        lookup = {}
        groups = self.folio_client.folio_get(
            "groups", key="usergroups", query_params={"limit": 99}
        )
        for g in groups:
            lookup[g['id']] = g['group']

        return lookup

    def reading_rooms_lookup(self) -> dict:
        lookup = {}
        rooms = self.folio_client.folio_get(
            "reading-room", key="readingRooms", query_params={"limit": 99}
        )
        for r in rooms:
            lookup[r['name']] = r['id']

        return lookup

    def usergroup_lookup(self) -> dict:
        lookup = {}
        query = None
        logger.info("Retrieving usergroup custom field options")
        with open(self.usergroup_sql_file()) as sqv:
            query = sqv.read()

        connection = Connection.get_connection_from_secrets("postgres_folio")
        conn_string = f"dbname=okapi user=okapi host={connection.host} port={connection.port} password={connection.password}"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        for opt in results[0][0]:
            lookup[opt['id']] = opt['value']

        return lookup

    def usergroup_sql_file(self, **kwargs) -> Path:
        sql_path = (
            Path(kwargs.get("airflow", "/opt/airflow"))
            / "libsys_airflow/plugins/folio/helpers/usergroups.sql"
        )

        return sql_path


@define
class FolioUser:
    id: str
    patronGroup: str
    customFields: dict
    readingRoomsData: ReadingRoomsData

    @property
    def patron_group_name(self) -> str:
        return self.readingRoomsData.patron_groups.get(self.patronGroup, "")

    @property
    def usergroup_name(self) -> str:
        return self.readingRoomsData.usergroups.get(
            self.customFields.get("usergroup", ""), ""
        )


def formatted_date(from_date: Union[str, None]) -> str:
    if not from_date:
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()
    else:
        formatted_date = from_date

    return formatted_date


@task(multiple_outputs=True)
def calculate_start_stop(div, job):
    output = {"start": int(div * job), "stop": int((job + 1) * div)}
    logger.info(f"Output in calculate_start_stop {output}")
    return output


@task
def retrieve_users_batch_for_reading_room_access() -> list:
    """Retrieve users with reading room access from FOLIO"""
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    from_date = params.get("from_date")

    client = folio_client()
    users = client.folio_get_all(
        "users",
        key="users",
        query=f'updatedDate>"{formatted_date(from_date)}"',
        limit=99999,
    )
    # return list(batched(users, 1000))
    return [row for row in users]


@task
def generate_reading_room_access(
    users: list, reading_rooms_data: ReadingRoomsData
) -> list:
    """Generate reading room access data for users in FOLIO"""
    rooms = reading_rooms_data.reading_rooms
    reading_room_patron_permissions = []

    logger.info(f"Generating reading room access for folio {len(users)} users")
    client = folio_client()
    for user in users:
        folio_user = FolioUser(
            id=user["id"],
            patronGroup=user.get("patronGroup", ""),
            customFields=user.get("customFields", {}),
            readingRoomsData=reading_rooms_data,
        )

        permissions = []
        existing_access = []
        existing_perms = client.folio_get(
            f"reading-room-patron-permission/{folio_user.id}"
        )
        for existing_perm in existing_perms:
            perm_id = existing_perm.get('id', str(uuid.uuid4()))
            existing_perm['id'] = perm_id
            del existing_perm['metadata']
            existing_access.append(existing_perm)

        for k, v in reading_rooms_config.items():
            access = "NOT_ALLOWED"
            for x in v['allowed'].keys():
                if folio_user.patron_group_name in v['allowed'][x]:
                    access = "ALLOWED"
            for x in v['disallowed'].keys():
                if folio_user.usergroup_name in v['disallowed'][x]:
                    access = "NOT_ALLOWED"

            """
            # Update existing access entry if one exists
            """
            for ea in existing_access:
                if ea['readingRoomName'] == k:
                    ea['access'] = access
            """
            All users should have an existing entry for each reading room when the
            reading room is created in FOLIO. However, to be safe, we check here and
            only add a new entry if one does not already exist.
            """
            if not any(ea.get('readingRoomName') == k for ea in existing_access):
                permissions.append(
                    {
                        "id": str(uuid.uuid4()),
                        "userId": folio_user.id,
                        "readingRoomId": rooms.get(k),
                        "readingRoomName": k,
                        "access": access,
                    }
                )

            permissions.extend(existing_access)

        patron_permissions = {"userId": folio_user.id, "permissions": permissions}
        reading_room_patron_permissions.append(patron_permissions)

    return reading_room_patron_permissions


@task
def update_reading_room_permissions(reading_room_patron_permissions: list):
    """Generate reading room access data for FOLIO"""
    client = folio_client()
    logger.info(
        f"Updating reading room patron permissions for {len(reading_room_patron_permissions)} users"
    )
    for patron_permission in reading_room_patron_permissions:
        logger.info(f"Updated reading room patron permissions: {patron_permission}")
        client.folio_put(
            f"reading-room-patron-permission/{patron_permission['userId']}",
            patron_permission['permissions'],
        )
