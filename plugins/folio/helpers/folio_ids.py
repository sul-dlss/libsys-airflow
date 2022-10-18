import json
import logging
import pathlib

from csv import DictReader

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


def _generate_barcode_call_number_map(tsv_path: pathlib.Path) -> dict:
    """
    Generates a dict of item barcodes to holdings call numbers
    """
    barcode_call_number_map = {}
    with tsv_path.open() as fo:
        dict_reader = DictReader(fo, delimiter="\t")
        for row in dict_reader:
            barcode_call_number_map[row["BARCODE"]] = row["BASE_CALL_NUMBER"]

    return barcode_call_number_map


def _lookup_holdings_uuid(
    barcode_call_number_map: dict, holdings_map: dict, barcode: str
) -> str:
    """
    Does a lookup to retrieve the call numbers for an item's barcode
    in order to extract the correct holdings uuid
    """
    # Creates a dict of call numbers to holdings uuid
    call_number_holdings = {}
    for uuid, info in holdings_map.items():
        call_number_holdings[info["call_number"]] = uuid
    item_call_number = barcode_call_number_map[barcode]
    return call_number_holdings[item_call_number]


def _update_holdings_map(mapping, hrid, uuid, holding) -> None:
    if len(mapping) < 1:
        return
    info = {"hrid": hrid, "call_number": holding["callNumber"], "items": []}
    if holding["id"] in mapping:
        mapping[holding["id"]][uuid] = info
    else:
        mapping[holding["id"]] = {uuid: info}


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
            _update_holdings_map(holdings_map, holdings_hrid, new_holdings_id, holding)
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
    task_instance = kwargs["task_instance"]
    iteration_dir = pathlib.Path(airflow) / f"migration/iterations/{dag.run_id}/"
    results_dir = iteration_dir / "results"
    okapi_url = Variable.get("OKAPI_URL")

    tsv_filename = task_instance.xcom_pull(
        task_ids="bib-files-group", key="tsv-base"
    ).split("/")[-1]

    items_lookup = _generate_barcode_call_number_map(
        iteration_dir / f"source_data/items/{tsv_filename}"
    )

    items_path = results_dir / "folio_items_transformer.json"

    with items_path.open() as fo:
        items = [json.loads(line) for line in fo.readlines()]

    with (results_dir / "holdings-items-map.json").open() as fo:
        holdings_map = json.load(fo)

    logger.info(f"Start updating identifiers for {len(items):,} items")
    with items_path.open("w+") as fo:
        for i, item in enumerate(items):
            original_holding_id = item["holdingsRecordId"]
            if len(holdings_map[original_holding_id]) == 1:
                # Only one holding exists so use
                holding_uuid = list(holdings_map[original_holding_id].keys())[0]
            else:
                # Retrieves the new Holding's UUID based on call number
                holding_uuid = _lookup_holdings_uuid(
                    items_lookup, holdings_map[original_holding_id], item["barcode"]
                )
            current_holding = holdings_map[original_holding_id][holding_uuid]
            holdings_hrid = current_holding["hrid"]
            current_count = len(current_holding["items"])
            item_hrid = f"{holdings_hrid[:1]}i{holdings_hrid[2:]}_{current_count + 1}"
            item["hrid"] = item_hrid
            item["holdingsRecordId"] = holding_uuid
            item_uuid = str(FolioUUID(okapi_url, FOLIONamespaces.items, item_hrid))
            item["id"] = item_uuid
            current_holding["items"].append(item_uuid)
            if not i % 1_000 and i > 0:
                logger.info(f"Generated uuids and hrids for {i:,} items")
            fo.write(f"{json.dumps(item)}\n")

    logger.info(f"Finished updating identifiers for {len(items):,} items")
