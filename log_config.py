from copy import deepcopy
from pydantic.utils import deep_update
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = deep_update(
    deepcopy(DEFAULT_LOGGING_CONFIG),
    {
        "loggers": {
            "airflow.task.operators.airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator": {
                "handlers": ["task"],
                "level": "WARN",
                "propagate": True,
            },
        }
    },
)
