import logging
import pathlib
from libsys_airflow.plugins.data_exports.marc.exporter import Exporter

logger = logging.getLogger(__name__)


def instance_files_dir(**kwargs) -> list[pathlib.Path]:
    """
    Finds the instance id directory for a vendor
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs.get("vendor")
    kind = kwargs.get("kind")
    instance_dir = (
        pathlib.Path(airflow) / f"data-export-files/{vendor}/instanceids/{kind}"
    )
    instance_files = list(instance_dir.glob("*.csv"))

    if not instance_files:
        logger.warning(f"Vendor instance files do not exist for {kind}")

    return instance_files


def marc_for_instances(**kwargs) -> dict:
    """
    Retrieves the converted marc for each instance id file in vendor directory
    """
    updates_and_deletes = {"updates": [], "deletes": []}  # type: dict

    for kind in ["updates", "deletes"]:
        instance_files = instance_files_dir(
            airflow=kwargs.get("airflow", "/opt/airflow"),
            vendor=kwargs.get("vendor", ""),
            kind=kind,
        )

        exporter = Exporter()
        marc_files = []
        for file_datename in instance_files:
            marc_file = exporter.retrieve_marc_for_instances(
                instance_file=file_datename, kind=kind
            )
            marc_files.append(marc_file)

        updates_and_deletes[kind].extend(str(f) for f in marc_files)

    return updates_and_deletes
