import asyncio
import contextlib
import logging
import pathlib
import libsys_airflow.plugins.folio.encumbrances.fix_encumbrances_quesnelia as fix_encumbrances_script

logger = logging.getLogger(__name__)


def fix_encumbrances_run(*args, **kwargs):
    choice = args[0]
    fiscal_year_code = args[1]
    tenant = args[2]
    username = args[3]
    password = args[4]

    airflow = kwargs.get("airflow", "/opt/airflow/")
    task_instance = kwargs["task_instance"]
    run_id = task_instance.run_id
    library = kwargs.get("library", "")
    log_path = pathlib.Path(airflow) / f"fix_encumbrances/{library}-{run_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("w+", 1) as log:
        with contextlib.redirect_stdout(log):
            try:
                asyncio.run(
                    fix_encumbrances_script.run_operation(
                        int(choice), fiscal_year_code, tenant, username, password
                    )
                )
            except Exception as e:
                logger.error(f"fix_encumbrance_script failed with: {e}")
                return str(log_path.absolute())

    return str(log_path.absolute())
