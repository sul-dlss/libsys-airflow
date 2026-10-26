import logging

from pathlib import Path

from libsys_airflow.plugins.data_exports.instance_ids import save_ids
from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances
from libsys_airflow.plugins.data_exports.marc.transforms import (
    add_holdings_items_to_marc_files,
    clean_and_serialize_marc_files,
)

logger = logging.getLogger(__name__)

VENDOR = "google_scanning"


def generate_marc_for_instances(instance_ids: list[str], filestamp: str) -> dict:
    """
    Generates a shipment's MARCXML from the resolved instance ids and its
    filestamp (staging.py::shipment_filestamp or
    caiasoft_shipment.py::sal3_filestamp).
    Must run inside a task context, since retrieve_marc_for_instances
    reads Airflow params via get_current_context().
    Returns the generated MARCXML path, the filestamp used (so
    generate_manifest can reuse the exact same one), and any instance ids
    that had no SRS record, for the shipment confirmation email.
    """
    unique_instance_ids = list(dict.fromkeys(instance_ids))
    instanceids_path = save_ids(
        vendor=VENDOR,
        kind="new",
        data=unique_instance_ids,
        timestamp=filestamp,
    )
    if not instanceids_path:
        raise ValueError("No instance ids to generate MARC for")

    marc_file_list = marc_for_instances(instance_files=[instanceids_path], as_xml=True)

    add_holdings_items_to_marc_files(marc_file_list, full_dump=False)
    clean_and_serialize_marc_files(marc_file_list)

    marc_files = marc_file_list.get("new", [])
    if not marc_files:
        raise ValueError(
            f"No MARC records generated from instance ids file {instanceids_path}"
        )

    return {
        "filestamp": filestamp,
        "marc_xml_path": str(Path(marc_files[0]).with_suffix(".xml")),
        "not_found_instance_ids": marc_file_list.get("not_found", []),
    }
