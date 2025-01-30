import pytest  # noqa


from libsys_airflow.plugins.authority_control.helpers import clean_up


def test_clean_up(mocker, tmp_path):
    authority_marc_file = tmp_path / "authority.mrc"
    authority_marc_file.touch()

    clean_up(str(authority_marc_file), airflow=str(tmp_path))

    assert authority_marc_file.exists() is False
    assert (tmp_path / "authorities/archive/authority.mrc").exists()
