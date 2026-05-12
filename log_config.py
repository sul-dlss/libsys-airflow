from copy import deepcopy
from pydantic.v1.utils import deep_update
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

REMOTE_TASK_LOG = None

LOGGING_CONFIG = deep_update(
    deepcopy(DEFAULT_LOGGING_CONFIG),
    {
        "loggers": {
            "airflow.hooks.base": {
                "handlers": ["task"],
                "level": "WARN",
                "propagate": True,
            },
        }
    },
)
