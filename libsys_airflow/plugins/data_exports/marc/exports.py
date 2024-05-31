import ast
import logging
import pathlib
from libsys_airflow.plugins.data_exports.marc.exporter import Exporter

logger = logging.getLogger(__name__)


def marc_for_instances(**kwargs) -> dict:
    """
    Retrieves the converted marc for each instance id file
    """
    instance_files = ast.literal_eval(kwargs.get("instance_files", "[]"))

    new_updates_deletes = {"new": [], "updates": [], "deletes": []}  # type: dict

    exporter = Exporter()
    marc_files = []
    for file_datename in instance_files:
        file_path = pathlib.Path(file_datename)
        kind = file_path.parent.stem
        marc_file = exporter.retrieve_marc_for_instances(
            instance_file=file_path, kind=kind
        )
        marc_files.append(marc_file)
        logger.info(f"Retrieved marc files {marc_file} for instance file {file_path}")
        new_updates_deletes[kind].extend(str(f) for f in marc_files)

    return new_updates_deletes
