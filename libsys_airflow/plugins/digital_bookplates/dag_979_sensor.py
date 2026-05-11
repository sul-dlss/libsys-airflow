import logging

import airflow_client.client
from airflow_client.client.models.dag_run_response import DAGRunResponse
from airflow_client.client.rest import ApiException
from airflow.sdk import BaseSensorOperator

from libsys_airflow.plugins.shared.utils import dag_run_response_url
from libsys_airflow.plugins.shared.airflow_api_client import (
    api_client,
)

logger = logging.getLogger(__name__)


class DAG979Sensor(BaseSensorOperator):

    def __init__(self, dag_runs: list, **kwargs):
        self.dag_runs: dict = {}
        for dag_run_id in dag_runs:
            self.dag_runs[dag_run_id] = {'state': None, 'instances': []}
        super().__init__(**kwargs)

    def poke(self, context) -> bool:
        logger.info(f"Checking state for {len(self.dag_runs)} DAG runs")
        client = api_client()
        for dag_run_id in self.dag_runs.keys():
            api_instance = airflow_client.client.DagRunApi(client)
            try:
                api_response: DAGRunResponse = api_instance.get_dag_run(
                    "digital_bookplate_979", dag_run_id
                )
            except ApiException as e:
                if e.status == 404:
                    logger.warning(
                        f"No dag run found for digital_bookplate_979 with run ID {dag_run_id}"
                    )
                else:
                    logger.info(
                        f"Exception when calling DagRunApi for {dag_run_id}: {e}"
                    )
                continue

            state = api_response.state.name.lower()
            if state in ['success', 'failed']:
                self.dag_runs[dag_run_id]['state'] = state
                self.dag_runs[dag_run_id]['url'] = dag_run_response_url(
                    dag_run=api_response
                )
                instances = []
                dag_run_config: dict = (
                    api_response.conf['druids_for_instance_id']
                    if api_response.conf
                    else {}
                )
                for instance, bookplates in dag_run_config.items():
                    funds = []
                    for bookplate in bookplates:
                        funds.append(
                            {
                                "name": bookplate.get("fund_name"),
                                "title": bookplate.get("title"),
                            }
                        )
                    instances.append({"uuid": instance, "funds": funds})

                self.dag_runs[dag_run_id]['instances'] = instances
        poke_result = all(
            [val['state'] in ['success', 'failed'] for val in self.dag_runs.values()]
        )
        logger.info(f"Result of polling DAGs {poke_result}")
        return poke_result
