import datetime
import logging


from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from folioclient import FolioClient
from plugins.folio.circ_rules import (
    friendly_batch_report,
    generate_batch_report,
    generate_batch_urls,
    policy_batch_report,
    policy_types,
    retrieve_batch_policies,
    setup_batch_rules,
)

logger = logging.getLogger(__name__)

folio_client = FolioClient(
    Variable.get("okapi_url"),
    "sul",
    Variable.get("folio_user"),
    Variable.get("folio_password"),
)


with DAG(
    "circ_rules_batch_tests",
    schedule_interval=None,
    start_date=datetime.datetime(2022, 12, 1),
    catchup=False,
    tags=["folio", "circ-rules"],
    max_active_runs=1,
) as dag:

    setup = PythonOperator(
        task_id="setup-circ-rules", python_callable=setup_batch_rules
    )

    with TaskGroup(group_id="retrieve-policies-group") as retrieve_policies_group:

        start_policy = DummyOperator(task_id="start-policies-retrieval")

        end_policy = DummyOperator(task_id="end-policies-retrieval")

        for policy_type in policy_types:
            rule_type_urls = PythonOperator(
                task_id=f"{policy_type}-generate-urls",
                python_callable=generate_batch_urls,
                op_kwargs={"folio_client": folio_client, "policy_type": policy_type},
            )

            retrieve_circ_policies = PythonOperator(
                task_id=f"{policy_type}-get-policies",
                python_callable=retrieve_batch_policies,
                op_kwargs={"folio_client": folio_client, "policy_type": policy_type},
            )

            start_policy >> rule_type_urls >> retrieve_circ_policies >> end_policy

    with TaskGroup(group_id="friendly-report-group") as report_group:

        friendly_labels = PythonOperator(
            task_id="friendly-report",
            python_callable=friendly_batch_report,
            op_kwargs={"folio_client": folio_client},
        )

        finish_reports = DummyOperator(task_id="end-reporting")

        for policy_type in policy_types:
            policy_tests = PythonOperator(
                task_id=f"{policy_type}-policy-test",
                python_callable=policy_batch_report,
                op_kwargs={"folio_client": folio_client, "policy_type": policy_type},
            )

            friendly_labels >> policy_tests >> finish_reports

    generate_circ_test_report = PythonOperator(
        task_id="generate-final-report", python_callable=generate_batch_report
    )

    finished_batch_report = DummyOperator(task_id="finished-test-batch")

    setup >> retrieve_policies_group >> report_group
    report_group >> generate_circ_test_report >> finished_batch_report
