import pathlib
from libsys_airflow.plugins.data_exports.marc.exporter import Exporter


def instance_files_dir(**kwargs) -> list[pathlib.Path]:
    """
    Finds the instance id directory for a vendor
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs.get("vendor")
    instance_dir = pathlib.Path(airflow) / f"data-export-files/{vendor}/instanceids"
    instance_files = list(instance_dir.glob("*.csv"))

    if not instance_files:
        raise ValueError("Vendor instance files do not exist")

    return instance_files


def marc_for_instances(**kwargs) -> list[str]:
    """
    Retrieves the converted marc for each instance id file in vendor directory
    """
    instance_files = instance_files_dir(
        airflow=kwargs.get("airflow", "/opt/airflow"), vendor=kwargs.get("vendor", "")
    )

    exporter = Exporter()
    marc_files = []
    for file_datename in instance_files:
        marc_file = exporter.retrieve_marc_for_instances(instance_file=file_datename)
        marc_files.append(marc_file)

    return [str(f) for f in marc_files]
