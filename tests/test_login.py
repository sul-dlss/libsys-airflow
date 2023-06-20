from libsys_airflow.plugins.folio.login import folio_login


def test_folio_login():
    # if the import is successful, this will always pass because the function ref is truthy, hence the typechecker complaint
    assert folio_login  # type: ignore
