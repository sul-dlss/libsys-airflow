import json
import logging
import pathlib

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
            instance_map[instance["id"]] = {"hrid": instance["hrid"], "holdings": {}}

    return instance_map


def _update_holding(**kwargs) -> None:
    """
    Iterates through a list of holdings, generates uuid and hrids based
    on the instance map, and optionally populates a holdings map for
    later item identifier generation
    """
    instance_map: dict = kwargs["instance_map"]
    holding: dict = kwargs["holding"]

    instance_id = holding["instanceId"]
    instance_hrid = instance_map[instance_id]["hrid"]
    current_count = len(instance_map[instance_id]["holdings"])
    holding_hrid = f"{instance_hrid[:1]}h{instance_hrid[1:]}_{current_count + 1}"

    holding["hrid"] = holding_hrid
    # For optimistic locking handling
    holding["_version"] = 1

    instance_map[instance_id]["holdings"][holding["id"]] = {
        "hrid": holding_hrid,
        "items": [],
    }


def generate_holdings_identifiers(**kwargs) -> None:
    """
    Loads FOLIO instances and holdings, generates and saves
    uuids and hrids for holdings
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    results_dir = pathlib.Path(airflow) / f"migration/iterations/{dag.run_id}/results/"

    instance_path = results_dir / "folio_instances_bibs-transformer.json"

    instance_map = _generate_instance_map(instance_path)

    with (results_dir / "folio_holdings.json").open("w+") as holdings_fo:
        for holdings_name in [
            "folio_holdings_tsv-transformer.json",
            "folio_holdings_electronic-transformer.json",
            "folio_holdings_boundwith.json",
        ]:
            logger.info(f"Starting holdings update for {holdings_name}")
            holdings_path = results_dir / holdings_name
            with holdings_path.open() as fo:
                for i, row in enumerate(fo.readlines()):
                    holding = json.loads(row)
                    _update_holding(holding=holding, instance_map=instance_map)
                    holdings_fo.write(f"{json.dumps(holding)}\n")
                    if not i % 1_000 and i > 0:
                        logger.info(
                            f"Generated holdings hrids for {i:,} in {holdings_name} "
                        )
    with (results_dir / "instance-holdings-items.json").open("w+") as fo:
        json.dump(instance_map, fo, indent=2)

    logger.info("Finished updating Holdings")


def generate_item_identifiers(**kwargs) -> None:
    """
    Loads FOLIO holdings map and items, generates and saves
    uuids and hrids for
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    iteration_dir = pathlib.Path(airflow) / f"migration/iterations/{dag.run_id}/"
    results_dir = iteration_dir / "results"

    items_path = results_dir / "folio_items_transformer.json"

    items = []
    with items_path.open() as fo:
        items = [json.loads(line) for line in fo.readlines()]

    with (results_dir / "instance-holdings-items.json").open() as fo:
        instances_map = json.load(fo)

    holdings_map = {}
    for instance_id, instance in instances_map.items():
        for uuid, holding in instance["holdings"].items():
            holding["instance"] = instance_id
            holdings_map[uuid] = holding

    logger.info(f"Start updating identifiers for {len(items):,} items")
    with items_path.open("w+") as fo:
        for i, item in enumerate(items):
            holding_id = item["holdingsRecordId"]
            if holding_id not in holdings_map:
                logger.error(f"{holding_id} not in found in Holdings")
                continue
            current_holding = holdings_map[holding_id]
            holdings_hrid = current_holding["hrid"]
            current_count = len(current_holding["items"])
            item_hrid = f"{holdings_hrid[:1]}i{holdings_hrid[2:]}_{current_count + 1}"
            item["hrid"] = item_hrid
            current_holding["items"].append(item["id"])
            instances_map[current_holding["instance"]]["holdings"][holding_id][
                "items"
            ].append(item["id"])
            if not i % 1_000 and i > 0:
                logger.info(f"Generated uuids and hrids for {i:,} items")
            fo.write(f"{json.dumps(item)}\n")

    with (results_dir / "instance-holdings-items.json").open("w+") as fo:
        json.dump(instances_map, fo)

    logger.info(f"Finished updating identifiers for {len(items):,} items")
