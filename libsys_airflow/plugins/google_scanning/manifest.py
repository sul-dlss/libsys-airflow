import logging

from libsys_airflow.plugins.google_scanning.constants import MARC_FILES_BASE

logger = logging.getLogger(__name__)


def manifest_filename(filestamp: str) -> str:
    return f"{filestamp}.txt"


def generate_manifest(barcode_cart_pairs: list[tuple[str, str]], filestamp: str) -> str:
    """
    Writes the shipment's cart-level manifest: tab-delimited barcode and
    cart name, no header row, ordered by cart then barcode -- so Google can
    match each scanned barcode back to the booktruck it came from across
    the merged set of selected carts. Written to marc-files/new/ using the
    same filestamp as the shipment's MARCXML (see
    marc.py::generate_shipment_marc/shipment_filestamp -- the caller must
    pass the filestamp that call returned, not recompute its own, so the
    manifest and MARCXML end up with matching names), so both files can be
    uploaded to Drive together.
    """
    manifest_dir = MARC_FILES_BASE / "new"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / manifest_filename(filestamp)

    ordered_pairs = sorted(barcode_cart_pairs, key=lambda pair: (pair[1], pair[0]))

    logger.info(
        f"Writing manifest for {len(ordered_pairs)} barcode(s) to {manifest_path}"
    )
    with manifest_path.open("w") as fo:
        for barcode, cart_name in ordered_pairs:
            fo.write(f"{barcode}\t{cart_name}\n")

    return str(manifest_path)
