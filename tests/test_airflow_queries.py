from airflow.models import DAG
from libsys_airflow.plugins.airflow.airflow_queries import get_dags


def get_dag_ids(dags: set[DAG] | list[DAG]) -> set[str]:
    return set([dag.dag_id for dag in dags])


"""
NOTE: these tests rely on the built-in Airflow example DAGs, which have tags, and which should be available
even in our test env, without a loaded up database being available.  This approach (as opposed to monkeypatching
and mocking the DagBag().dags property value) is fragile in the sense that fluctuating example DAGs between
Airflow versions could break the tests, but it also fully exercises an actual DagBag().dags call without mocking, which
is nice since DagBag().dags is not (as of Airflow 2.6.0) a publicly documented property in the SDK, despite not using
the '_' private naming convention.  See https://airflow.apache.org/docs/apache-airflow/2.6.0/_modules/airflow/models/dagbag.html#DagBag
"""


def test_get_dags_filtering_one_actual_tag():
    assert get_dag_ids(get_dags(filter_tags=set(['dataset-scheduled']))) == set(
        [
            'dataset_produces_2',
            'dataset_consumes_unknown_never_scheduled',
            'dataset_consumes_1_and_2',
            'dataset_consumes_1_never_scheduled',
            'dataset_consumes_1',
            'dataset_produces_1',
        ]
    )


def test_get_dags_filtering_multiple_actual_tags():
    assert get_dag_ids(get_dags(filter_tags=set(['example', 'example3']))) == set(
        [
            'latest_only',
            'example_task_group_decorator',
            'example_complex',
            'example_python_operator',
            'tutorial_taskflow_api_virtualenv',
            'example_bash_operator',
            'example_short_circuit_operator',
            'example_branch_operator',
            'tutorial_taskflow_api',
            'example_task_group',
            'tutorial',
            'example_passing_params_via_test_command',
            'latest_only_with_trigger',
            'example_time_delta_sensor_async',
            'example_dag_decorator',
            'example_xcom',
            'example_xcom_args_with_operators',
            'example_xcom_args',
            'example_branch_python_operator_decorator',
            'example_weekday_branch_operator',
            'example_skip_dag',
            'example_short_circuit_decorator',
            'tutorial_dag',
            'example_trigger_target_dag',
            'example_sensor_decorator',
            'example_branch_dop_operator_v3',
            'example_sensors',
            'example_branch_datetime_operator_3',
            'example_branch_datetime_operator',
            'example_branch_datetime_operator_2',
            'example_nested_branch_dag',
            'example_subdag_operator',
            'example_trigger_controller_dag',
            'example_setup_teardown_taskflow',
            'tutorial_objectstorage',
            'example_python_decorator',
            'example_setup_teardown',
        ]
    )


def test_get_dags_filtering_mixed_actual_and_nonexistent_tags():
    assert get_dag_ids(
        get_dags(filter_tags=set(['consumes', 'fooobaaaarbaaaazzz']))
    ) == set(
        [
            'dataset_consumes_1_and_2',
            'dataset_consumes_1_never_scheduled',
            'dataset_consumes_1',
        ]
    )


def test_get_dags_nonexistent_tags():
    assert (
        get_dag_ids(
            get_dags(filter_tags=set(['fooobaaaarbaaaazzz', 'foo-bar-and-grill']))
        )
        == set()
    )


def test_get_dags_no_tags():
    assert get_dag_ids(get_dags(filter_tags=set())) == set()
