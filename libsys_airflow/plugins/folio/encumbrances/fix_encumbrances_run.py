import asyncio
import contextlib
import logging
import pathlib
import libsys_airflow.plugins.folio.encumbrances.fix_encumbrances as fix_encumbrances_script


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
        logging.basicConfig(level=logging.INFO, filename=log)
        with contextlib.redirect_stdout(OutputLogger()):
            asyncio.run(
                fix_encumbrances_script.run_operation(
                    int(choice), fiscal_year_code, tenant, username, password
                )
            )

    return str(log_path.absolute())


class OutputLogger:
    def __init__(self, name="root", level="INFO"):
        self.logger = logging.getLogger(name)
        self.level = getattr(logging, level)

    def write(self, msg):
        if msg and not msg.isspace():
            self.logger.log(self.level, msg)

    def flush(self):
        pass
