import logging

logger = logging.getLogger(__name__)


def dispatched_shipments(manifest: dict) -> list[dict]:
    """
    Filters a courier manifest response down to shipments CaiaSoft has
    actually dispatched -- shipments still "LOADING" haven't left CaiaSoft
    yet, so their barcodes aren't ready to ship to Google.

    The API returns "DISPATCH" even though the API guide's field table says
    "DISPATCHED"; match what the API actually sends.
    """
    return [
        shipment
        for shipment in manifest.get("manifest", [])
        if shipment.get("shipment_status") == "DISPATCH"
    ]


def _carts(shipment: dict) -> list[dict]:
    """
    CaiaSoft returns carts either as a list or as an object keyed by cart
    number (e.g. {"1": {...}}); normalize both to a list.
    """
    carts = shipment.get("carts") or []
    if isinstance(carts, dict):
        return list(carts.values())
    return carts


def barcode_bin_pairs(shipments: list[dict]) -> list[tuple[str, str]]:
    """
    Flattens dispatched shipments' carts into (barcode, bin) pairs -- the
    CaiaSoft equivalent of the on-campus shipment flow's (barcode,
    cart_name) pairs, so it plugs directly into
    shipment.py::resolve_instance_ids and manifest.py::generate_manifest.
    """
    pairs: list[tuple[str, str]] = []
    for shipment in shipments:
        for cart in _carts(shipment):
            bin_id = cart.get("bin") or ""
            for barcode in cart.get("items", []):
                pairs.append((barcode, bin_id))
    return pairs


def sal3_filestamp(date_str: str) -> str:
    """
    Builds this run's file stem for the SAL3 shipment's MARCXML/manifest,
    e.g. "stanford_20260813-sal3". Unlike the on-campus shipment's
    filestamp, no time suffix is needed since this DAG processes at most
    one date per run.
    """
    return f"stanford_{date_str}-sal3"
