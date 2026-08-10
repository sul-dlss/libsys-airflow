import logging

from libsys_airflow.plugins.google_scanning.constants import STAGED_FILES_BASE

logger = logging.getLogger(__name__)

# Sibling of STAGED_FILES_BASE/ARCHIVED_FILES_BASE, matching the marc-files
# directory that marc_for_instances/add_holdings_items_to_marc_files/
# clean_and_serialize_marc_files (plugins/data_exports/marc/) write the
# shipment's MARCXML into, so the manifest lands next to it for upload.
MARC_FILES_BASE = STAGED_FILES_BASE.parent / "marc-files"


def manifest_filename(shipped_at: str) -> str:
    return f"stanford_{shipped_at}-campus.txt"


def generate_manifest(
    barcode_cart_pairs: list[tuple[str, str]], shipped_at: str
) -> str:
    """
    Writes the shipment's cart-level manifest: tab-delimited barcode and
    cart name, no header row, ordered by cart then barcode -- so Google can
    match each scanned barcode back to the booktruck it came from across
    the merged set of selected carts. Written to marc-files/new/ using the
    same stanford_YYYYMMDD-campus naming convention as the shipment's
    MARCXML, so both files can be uploaded to Drive together.
    """
    manifest_dir = MARC_FILES_BASE / "new"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / manifest_filename(shipped_at)

    ordered_pairs = sorted(barcode_cart_pairs, key=lambda pair: (pair[1], pair[0]))

    logger.info(
        f"Writing manifest for {len(ordered_pairs)} barcode(s) to {manifest_path}"
    )
    with manifest_path.open("w") as fo:
        for barcode, cart_name in ordered_pairs:
            fo.write(f"{barcode}\t{cart_name}\n")

    return str(manifest_path)
