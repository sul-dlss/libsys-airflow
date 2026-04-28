import logging
import airflow_client.client
from airflow_client.client.rest import ApiException
from airflow_client.client.models.connection_body import ConnectionBody
from urllib.parse import urlparse
from typing import Optional
import json

from airflow.sdk import task
from libsys_airflow.plugins.shared.airflow_api_client import (
    api_client,
)
from libsys_airflow.plugins.folio.interface import interface_info

logger = logging.getLogger(__name__)

CONN_TYPES = ["ftp", "sftp"]


@task
def create_connection_task(interface_id: str) -> str:
    """
    Given a vendor_interface_uuid, returns the Airflow ConnectionBody.connection_id
    """
    return create_connection(interface_id)


def create_connection(interface_id: str) -> str:
    info = interface_info(interface_id=interface_id)
    uri = urlparse(info["uri"])
    if uri.scheme not in CONN_TYPES:
        raise ValueError(f"Unknown connection type for {interface_id}: {uri.scheme}")

    conn_id = find_or_create_conn(
        uri.scheme,
        uri.hostname,
        info["username"],
        info["password"],
        uri.port,
        _key_file(info, uri),
    )
    logger.info(f"Connection id for Interface {interface_id}: {conn_id}")
    return conn_id


def find_or_create_conn(
    conn_type: str,
    host: str,
    login: str,
    pwd: Optional[str] = None,
    port: Optional[int] = None,
    key_file: Optional[str] = None,
) -> str:
    conn_id = f"{conn_type}-{host}-{login}"
    client = api_client()
    api_instance = airflow_client.client.ConnectionApi(client)
    try:
        api_response = api_instance.get_connection(
            connection_id=conn_id, _headers={"Accept": "application/json"}
        )
        return api_response.connection_id
    except ApiException as e:
        logger.info(f"Exception when calling ConnectionApi for {conn_id}: {e}")

    conn: dict = {}
    conn["connection_id"] = conn_id
    conn["conn_type"] = conn_type
    conn["host"] = host
    conn["login"] = login
    conn["port"] = port

    if pwd:
        conn["password"] = pwd
    if key_file:
        conn["extra"] = json.dumps({"key_file": key_file})

    payload = ConnectionBody.from_dict(conn)
    if payload is None:
        raise ValueError(
            f"Invalid connection data for conn_id: {conn_id}. Payload cannot be None."
        )
    try:
        api_response = api_instance.post_connection(payload)
        return api_response.connection_id
    except ApiException as e:
        logger.warning(f"Exception when posting ConnectionBody for {conn_id}: {e}")

    return api_response.connection_id


def _key_file(info, uri):
    if info["password"]:
        return None

    # By convention, using the hostname as the key file name
    return f"/opt/airflow/vendor-keys/{uri.hostname}"
