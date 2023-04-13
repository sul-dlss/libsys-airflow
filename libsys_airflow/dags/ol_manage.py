import logging
import time

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.dagrun import DagRun

from libsys_airflow.plugins.folio.db import add_inventory_triggers, drop_inventory_triggers

logger = logging.getLogger(__name__)


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 7, 19),
    catchup=False,
    max_active_runs=1,
    tags=["folio", "bib_import"],
)
def optimistic_locking_management():
    """
    ## Manages FOLIO's Optimistic Locking
    DAG should be triggered before any symphony_marc_import DAG runs. Drops
    OL triggers on mod_inventory_storage and then monitors for current
    symphony_marc_import. If there are not any active symphony_marc_import DAG
    runs, waits 15 minutes before creating OL triggers.
    """

    @task
    def drop_ol_triggers():
        logger.info("Dropping FOLIO's optimistic locking for inventory")
        drop_inventory_triggers()
        logger.info("Finished dropping inventory triggers")

    @task
    def monitor_dag_runs():
        logger.info("Monitoring symphony_marc_import active dag runs")
        not_finished = True
        while 1:
            time.sleep(30)
            dag_runs = DagRun.active_runs_of_dags(dag_ids=["symphony_marc_import"])
            logger.info(
                f"Total number of symphony_marc_import dag runs {len(dag_runs)}"
            )
            if len(dag_runs) < 1 and not_finished is True:
                logger.info("Sleeping for 5 minutes")
                time.sleep(300)
                logger.info("Finished sleeping")
                not_finished = False
                continue
            if len(dag_runs) < 1:
                logger.info("Finished, no active symphony_marc_import dag runs")
                return
            else:
                not_finished = True

    @task
    def create_ol_triggers():
        logger.info("Creating triggers for FOLIO's inventory optimistic locking")
        add_inventory_triggers()
        logger.info(
            "Finished creating triggers for FOLIO's inventory optimistic locking"
        )

    drop_ol_triggers() >> monitor_dag_runs() >> create_ol_triggers()


inventory_ol_manage = optimistic_locking_management()
