import ast
import logging
import pathlib
from libsys_airflow.plugins.data_exports.marc.exporter import Exporter

logger = logging.getLogger(__name__)


def marc_for_instances(**kwargs) -> dict:
    """
    Retrieves the converted marc for each instance id file
    """
    instance_files = kwargs.get("instance_files", [])
    if isinstance(instance_files, str):
        instance_files = ast.literal_eval()

    new_updates_deletes = {"new": [], "updates": [], "deletes": []}  # type: dict

    exporter = Exporter()
    
    for file_datename in instance_files:
        file_path = pathlib.Path(file_datename)
        kind = file_path.parent.stem
        marc_file = exporter.retrieve_marc_for_instances(
            instance_file=file_path, kind=kind
        )
        logger.info(f"Retrieved marc files {marc_file} for instance file {file_path}")
<<<<<<< HEAD
        new_updates_deletes[kind].extend(str(f) for f in marc_files if kind in str(f))
=======
        new_updates_deletes[kind].append(str(marc_file))
>>>>>>> 6a3dd854 (Adjusts tasks dependencies to support parallel New and Deletes tasks)

    return new_updates_deletes
