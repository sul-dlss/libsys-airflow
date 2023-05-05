# needed to import packages in the plugin

import pathlib
import sys

# for tests that need to interact with the Airflow webapp
from client import test_client

root_directory = pathlib.Path(__file__).parent.parent
dir = root_directory / "libsys_airflow"

vendor_app_templates = f"{root_directory}/libsys_airflow/plugins/vendor_app/templates"

sys.path.append(str(dir))

