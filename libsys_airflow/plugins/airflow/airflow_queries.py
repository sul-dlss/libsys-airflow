import logging

from airflow.models import DagBag, DAG


logger = logging.getLogger(__name__)


def get_dags(filter_tags: set[str]) -> set[DAG]:
    """
    Return the set of DAGs that match at least one of the filter criteria.
    * Approach lightly adapted from https://stackoverflow.com/questions/49983034/get-dag-from-airflow (which lists all the tasks in all the DAGs).
    * An alternate approach would be to use the REST API via apache-airflow-client, since it offers a way to list DAGs
      filtered by tag, see https://airflow.apache.org/docs/apache-airflow/2.6.0/stable-rest-api-ref.html#operation/get_dags (that
      would require settling on a REST API authN approach).
    * Warning: When DagBag().dags is called, warnings are logged about DAGs and tasks already being loaded, but manual
      testing indicates that no trouble is caused by the call (i.e. all the DAGs still show up in the web UI).  This should
      be fine if we only read from DagBag().dags.  We should likely avoid ever directly setting DagBag().dags ourselves, or we
      should at least test any such approach thoroughly and see if Airflow docs or source indicate against such usage.
    * dag.tags is list[str], hence the cast to set.
    """
    return set(
        [dag for dag in DagBag().dags.values() if len(set(dag.tags) & filter_tags) > 0]
    )
