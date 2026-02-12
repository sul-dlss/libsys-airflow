# needed to import packages in the plugin

import os
import pathlib
import sys


root_directory = pathlib.Path(__file__).parent.parent
dir = root_directory / "libsys_airflow"

sys.path.append(str(dir))

# Set AIRFLOW_CONFIG to use the local airflow.cfg file for tests
os.environ["AIRFLOW_CONFIG"] = str(root_directory / "airflow.cfg")

# Set Airflow API base URL for tests
os.environ["AIRFLOW__API__BASE_URL"] = "http://localhost:8080"
