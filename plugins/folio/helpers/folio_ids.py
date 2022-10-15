from airflow.models import Variable
from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID

def generate_holdings_identifiers(
    holdings: list,
    instance_map: dict,
    holdings_map: dict=None,
) -> None:
    for holding in holdings:
        instance_id = holding["instanceId"]
        instance_hrid = instance_map[instance_id]["hrid"]
        current_count = len(instance_map[instance_id]["holdings"])
        holdings_hrid = f"{instance_hrid[:1]}h{instance_hrid[1:]}_{current_count + 1}"
        holding["hrid"] = holdings_hrid
        new_holdings_id = str(FolioUUID(
            Variable.get("OKAPI_URI"),
            FOLIONamespaces.holdings,
            holdings_hrid
        ))
        if holdings_map:
            holdings_map[holding["id"]] = { 
                "new": new_holdings_id,
                "hrid": holdings_hrid,
                "items": []
            }
        holding["id"] = new_holdings_id
        instance_map[instance_id]["holdings"].append(new_holdings_id)


def generate_item_identifiers(
    items: list,
    holdings_map: dict
    ) -> None:
    for item in items:
        holding_id = item['holdingsRecordId']
        holdings_hrid = holdings_map[holding_id]["hrid"]
        current_count = len(holdings_map[holding_id]["items"])
        item_hrid = f"{holdings_hrid[:1]}i{holdings_hrid[2:]}_{current_count + 1}"
        item["hrid"] = item_hrid
        item_uuid = str(
            FolioUUID(
                "https://okapiurl.edu",
                FOLIONamespaces.items,
                item_hrid
            )
        )
        item["id"] = item_uuid
        holdings_map[holding_id]["items"].append(item_uuid)
