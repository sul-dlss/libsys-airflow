import pytest

from airflow.models import DagBag

@pytest.fixture()
def dagbag():
    return DagBag("libsys_airflow/dags")

def test_gobi(dagbag):
    dag = dagbag.get_dag(dag_id="gobi")
    assert dag is not None
    assert len(dag.tasks) == 15 