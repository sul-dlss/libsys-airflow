from airflow.models import Variable


def is_production():
    return bool(Variable.get("OKAPI_URL").find("prod") > 0)
