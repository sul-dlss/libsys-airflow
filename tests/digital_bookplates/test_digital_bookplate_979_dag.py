from libsys_airflow.dags.digital_bookplates.digital_bookplate_979 import (
    digital_bookplate_979,
)


def test_reruns_with_latest_version():
    dag = digital_bookplate_979()

    assert dag.rerun_with_latest_version is True
