from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

from libsys_airflow.plugins.digital_bookplates.dag_979_sensor import DAG979Sensor
from libsys_airflow.plugins.digital_bookplates.email import summary_add_979_dag_runs

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@task(multiple_outputs=True)
def retrieve_variables(**kwargs):
    params = kwargs['params']
    dag_runs = params.get("dag_runs", [])
    addl_email = params.get("email")
    return {"dag_runs": dag_runs, "email": addl_email}


@task
def poll_979_dags(**kwargs):
    dag_runs = kwargs.get('dag_runs', [])
    # Checks every 10 seconds
    sensor = DAG979Sensor(
        task_id="poll-979-dags", dag_runs=dag_runs, poke_interval=10.0
    )
    sensor.execute(kwargs)
    return sensor.dag_runs


@dag(
    default_args=default_args,
    start_date=datetime(2024, 10, 15),
    schedule=None,
    catchup=False,
    tags=["digital bookplates"],
    max_active_runs=3,
)
def poll_for_digital_bookplate_979s_email():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    vars = retrieve_variables()

    poll_result = poll_979_dags(dag_runs=vars["dag_runs"])

    email = summary_add_979_dag_runs(dag_runs=poll_result, email=vars['email'])

    start >> vars

    email >> end


poll_for_digital_bookplate_979s_email()
