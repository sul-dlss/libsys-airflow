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
    booktruck it came from. Written to marc-files/manifests/ using the
    filestamp marc.py::generate_marc_for_instances returned, so the
    manifest and MARCXML have matching names and the
    send_google_scanning_files DAG uploads them to Drive together.
    """
    manifest_dir = MARC_FILES_BASE / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / manifest_filename(filestamp)

    logger.info(
        f"Writing manifest for {len(barcode_cart_pairs)} barcode(s) "
        f"to {manifest_path}"
    )
    with manifest_path.open("w") as fo:
        for barcode, cart_name in barcode_cart_pairs:
            fo.write(f"{cart_name}\t{barcode}\n")

    return str(manifest_path)
