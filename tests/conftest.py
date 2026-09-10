# needed to import packages in the plugin

import os
import pathlib
import sys
import tempfile


root_directory = pathlib.Path(__file__).parent.parent
dir = root_directory / "libsys_airflow"

sys.path.append(str(dir))

# Set AIRFLOW_CONFIG to use the local airflow.cfg file for tests
os.environ["AIRFLOW_CONFIG"] = str(root_directory / "airflow.cfg")

# Set Airflow API base URL for tests
os.environ["AIRFLOW__API__BASE_URL"] = "http://localhost:8080"

# SimpleAuthManager.init() writes a plaintext passwords file, defaulting to one inside
# AIRFLOW_HOME. Keep that out of the developer's Airflow home and out of the repo.
os.environ.setdefault(
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
    str(pathlib.Path(tempfile.gettempdir()) / "libsys_airflow_test_passwords.json"),
)

# The plugin apps are behind require_view_access, which calls get_auth_manager(). That
# raises unless the manager has been initialized, which normally happens on API server
# startup. airflow.cfg deliberately leaves [core] auth_manager unset, so this gives us
# Airflow's default, SimpleAuthManager, and the real authorization code path rather than
# a mock. Imported here rather than at the top of the file because it reads the
# environment above.
from airflow.api_fastapi.app import init_auth_manager  # noqa: E402

init_auth_manager()
