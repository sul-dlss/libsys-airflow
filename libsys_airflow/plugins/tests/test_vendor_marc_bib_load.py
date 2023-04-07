import pytest

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag("dags")


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="vendor_marc_bib_load")
    assert dag is not None
    assert len(dag.tasks) == 3
