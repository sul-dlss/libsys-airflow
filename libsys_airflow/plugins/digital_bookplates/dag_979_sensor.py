import logging

from airflow.models import DagRun
from airflow.sensors.base_sensor_operator import BaseSensorOperator

logger = logging.getLogger(__name__)


class DAG979Sensor(BaseSensorOperator):

    def __init__(self, dag_runs: list, **kwargs):
        self.dag_runs: dict = {}
        for dag_run_id in dag_runs:
            self.dag_runs[dag_run_id] = {'state': None, 'instance_uuid': []}
        super().__init__(**kwargs)

    def poke(self, context) -> bool:
        logger.info(f"Checking state for {len(self.dag_runs)} DAG runs")
        for dag_run_id in self.dag_runs.keys():
            dag_runs = DagRun.find(dag_id='digital_bookplate_979', run_id=dag_run_id)
            if len(dag_runs) < 1:
                continue
            dag_run = dag_runs[0]
            self.dag_runs[dag_run_id]['state'] = dag_run.get_state()
            self.dag_runs[dag_run_id]['instance_uuids'] = list(
                dag_run.conf['druids_for_instance_id'].keys()
            )
        poke_result = all(
            [val['state'] in ['success', 'failed'] for val in self.dag_runs.values()]
        )
        logger.info(f"Result of polling DAGs {poke_result}")
        return poke_result
