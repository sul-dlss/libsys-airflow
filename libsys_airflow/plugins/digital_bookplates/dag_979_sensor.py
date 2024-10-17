import logging

from airflow.models import DAG, DagRun
from airflow.sensors.base_sensor_operator import BaseSensorOperator

logger = logging.getLogger(__name__)

class DAG979Sensor(BaseSensorOperator):

    def __init__(self, dag_runs: list, **kwargs):
        self.dag_runs = {}
        for dag_run_id in dag_runs:
            self.dag_runs[dag_run_id] = None
        super().__init__(**kwargs)

    def poke(self, context) -> bool:
        logger.info("In POKE for DAG 979")
        dag = DAG(dag_id='digital_bookplate_979')
        last_dag_run = dag.get_last_dagrun(include_externally_triggered=True)
        logger.info(f"{dag} DAG {dag.has_dag_runs()} last run is {last_dag_run.run_id}")
        for i,dag_run_id in enumerate(self.dag_runs.keys()):
            dag_run = DagRun(
                dag_id='digital_bookplate_979',
                run_id=last_dag_run.run_id)
            logger.info(f"{dag_run_id} status is {dag_run.state} {dag_run}")
            self.dag_runs[dag_run_id] = dag_run.get_state()
        return all([val in ['SUCCESS', 'FAILED'] for val in self.dag_runs.values()])

