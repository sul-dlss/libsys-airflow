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
        instance_files = ast.literal_eval(instance_files)

    new_updates_deletes = {"new": [], "updates": [], "deletes": []}  # type: dict

    exporter = Exporter()

    for file_datename in instance_files:
        if not file_datename:
            continue
        file_path = pathlib.Path(file_datename)
        kind = file_path.parent.stem
        marc_file = exporter.retrieve_marc_for_instances(
            instance_file=file_path, kind=kind
        )
        marc_file_str = str(marc_file)

        if len(marc_file_str) < 1:
            continue
        logger.info(
            f"Retrieved marc files {marc_file_str} for instance file {file_path}"
        )
        new_updates_deletes[kind].append(marc_file_str)

    return new_updates_deletes
