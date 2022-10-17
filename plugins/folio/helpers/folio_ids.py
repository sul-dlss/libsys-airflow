import json
import logging
import pathlib

from airflow.models import Variable
from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID

logger = logging.getLogger(__name__)


def _generate_instance_map(instance_path: pathlib.Path) -> dict:
    """
    Takes FOLIO instance files and returns a dictionary by
    uuid keys with hrid and holdings list values
    """
    instance_map = {}
    with instance_path.open() as fo:
        for line in fo.readlines():
            instance = json.loads(line)
            instance_map[instance["id"]] = {"hrid": instance["hrid"], "holdings": []}

    return instance_map


def _update_holding_ids(
    holdings_path: pathlib.Path,
    instance_map: dict,
    okapi_url: str,
    holdings_map: dict = {},
) -> None:
    """
    Iterates through a list of holdings, generates uuid and hrids based
    on the instance map, and optionally populates a holdings map for
    later item identifier generation
    """
    with holdings_path.open() as fo:
        holdings = [json.loads(line) for line in fo.readlines()]

    with holdings_path.open("w+") as fo:
        for i, holding in enumerate(holdings):
            instance_id = holding["instanceId"]
            instance_hrid = instance_map[instance_id]["hrid"]
            current_count = len(instance_map[instance_id]["holdings"])
            holdings_hrid = (
                f"{instance_hrid[:1]}h{instance_hrid[1:]}_{current_count + 1}"
            )
            holding["hrid"] = holdings_hrid
            new_holdings_id = str(
                FolioUUID(okapi_url, FOLIONamespaces.holdings, holdings_hrid)
            )
            if len(holdings_map) > 0:
                holdings_map[holding["id"]] = {
                    "new": new_holdings_id,
                    "hrid": holdings_hrid,
                    "items": [],
                }
            holding["id"] = new_holdings_id
            # For optimistic locking handling
            holding["_version"] = 1
            instance_map[instance_id]["holdings"].append(new_holdings_id)
            fo.write(f"{json.dumps(holding)}\n")
            if not i % 1_000 and i > 0:
                logger.info(f"Generated uuids and hrids for {i:,} holdings")

    # Persists holdings_map if populated
    if len(holdings_map) > 0:
        holdings_map_path = holdings_path.parent / "holdings-items-map.json"
        with holdings_map_path.open("w+") as fo:
            json.dump(holdings_map, fo)

    logger.info(f"Finished updating {i:,} for {holdings_path} ")


def generate_holdings_identifiers(**kwargs) -> None:
    """
    Loads FOLIO instances and holdings, generates and saves
    uuids and hrids for holdings
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    results_dir = pathlib.Path(airflow) / f"migration/iterations/{dag.run_id}/results/"
    okapi_url = Variable.get("OKAPI_URL")

    instance_path = results_dir / "folio_instances_bibs-transformer.json"

    instance_map = _generate_instance_map(instance_path)
    tsv_holdings_path = results_dir / "folio_holdings_tsv-transformer.json"

    # Adds a stub key-value holdings map to populate from base tsv file
    _update_holding_ids(
        tsv_holdings_path, instance_map, okapi_url, {"type": "base tsv"}
    )
    logger.info(f"Finished updating tsv holdings {tsv_holdings_path}")

    # Updates MHLD holdings
    mhld_holdings_path = results_dir / "folio_holdings_mhld-transformer.json"
    _update_holding_ids(mhld_holdings_path, instance_map, okapi_url)
    logger.info(f"Finished updating mhls holdings {mhld_holdings_path}")

    # Updates Electronic holdings
    electronic_holdings_path = (
        results_dir / "folio_holdings_electronic-transformer.json"
    )
    _update_holding_ids(electronic_holdings_path, instance_map, okapi_url)
    logger.info(f"Finished updating electronic holdings {electronic_holdings_path}")


def generate_item_identifiers(**kwargs) -> None:
    """
    Loads FOLIO holdings map and items, generates and saves
    uuids and hrids for
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    results_dir = pathlib.Path(airflow) / f"migration/iterations/{dag.run_id}/results/"
    okapi_url = Variable.get("OKAPI_URL")

    items_path = results_dir / "folio_items_transformer.json"

    with items_path.open() as fo:
        items = [json.loads(line) for line in fo.readlines()]

    with (results_dir / "holdings-items-map.json").open() as fo:
        holdings_map = json.load(fo)

    with items_path.open("w+") as fo:
        for i, item in enumerate(items):
            holding_id = item["holdingsRecordId"]
            current_holding = holdings_map[holding_id]
            holdings_hrid = current_holding["hrid"]
            current_count = len(current_holding["items"])
            item_hrid = f"{holdings_hrid[:1]}i{holdings_hrid[2:]}_{current_count + 1}"
            item["hrid"] = item_hrid
            item["holdingsRecordId"] = current_holding["new"]
            item_uuid = str(FolioUUID(okapi_url, FOLIONamespaces.items, item_hrid))
            item["id"] = item_uuid
            current_holding["items"].append(item_uuid)
            if not i % 1_000 and i > 0:
                logger.info(f"Generated uuids and hrids for {i:,} items")
            fo.write(f"{json.dumps(item)}\n")

    logger.info(f"Finished updating identifiers for {len(items):,} items")
