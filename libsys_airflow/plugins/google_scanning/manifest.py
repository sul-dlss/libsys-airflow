import logging

from libsys_airflow.plugins.google_scanning.constants import MARC_FILES_BASE

logger = logging.getLogger(__name__)


def manifest_filename(filestamp: str) -> str:
    return f"{filestamp}.txt"


def generate_manifest(barcode_cart_pairs: list[tuple[str, str]], filestamp: str) -> str:
    """
    Writes the shipment's cart-level manifest: tab-delimited cart name and
    barcode, no header row, in the order given -- the order the barcodes
    sit on the cart, so Google can match each scanned barcode back to the
    booktruck it came from. Written to marc-files/manifests/,
    using the same filestamp as the shipment's MARCXML (see
    marc.py::generate_shipment_marc/shipment_filestamp -- the caller must
    pass the filestamp that call returned, not recompute its own, so the
    manifest and MARCXML end up with matching names), so both files can be
    uploaded to Drive together.
    """
    manifest_dir = MARC_FILES_BASE / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / manifest_filename(filestamp)

    logger.info(
        f"Writing manifest for {len(barcode_cart_pairs)} barcode(s) to {manifest_path}"
    )
    with manifest_path.open("w") as fo:
        for barcode, cart_name in barcode_cart_pairs:
            fo.write(f"{cart_name}\t{barcode}\n")

    return str(manifest_path)
