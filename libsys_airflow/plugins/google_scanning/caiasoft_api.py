import logging

import httpx

from airflow.sdk import Connection

logger = logging.getLogger(__name__)

CAIASOFT_CONN_ID = "caiasoft_api"


class CaiaSoftAPIWrapper:
    """
    REST client for CaiaSoft's courier manifest/shipment endpoints, used to
    retrieve SAL3 items dispatched to Google for scanning.
    See https://portal.caiasoft.com/apiguide.php?serv=restapi&rec=3&oper=couriermanifest
    and https://portal.caiasoft.com/apiguide.php?serv=restapi&rec=3&oper=couriershipment
    """

    def __init__(self, conn_id: str = CAIASOFT_CONN_ID):
        connection = Connection.get(conn_id)
        self.base_url = connection.host
        self.headers = connection.extra_dejson

    def courier_manifest(
        self, ship_from: str, ship_to: str, courier: str = "GOOGLE"
    ) -> dict:
        """
        Retrieves every shipment dispatched to courier between ship_from and
        ship_to (both YYYYMMDD), including each shipment's carts/bins and
        item barcodes.
        """
        url = f"{self.base_url}/api/couriermanifest/v1/{ship_from}/{ship_to}/{courier}"
        with httpx.Client(headers=self.headers) as client:
            response = client.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
