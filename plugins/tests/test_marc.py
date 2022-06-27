import pytest  # noqa

from plugins.folio.marc import post_marc_to_srs, remove_srs_json


def test_remove_srs_json(tmp_path):
    results_dir = tmp_path / "migration/results"
    results_dir.mkdir(parents=True)

    srs_filepath = results_dir / "test-srs.json"

    srs_filepath.write_text(
        """{ "id": "e9a161b7-3541-54d6-bd1d-e4f2c3a3db79",
      "rawRecord": {"content": "{\"leader\": \"01634pam a2200433 i 4500\", }}}"""
    )

    remove_srs_json(airflow=tmp_path, srs_filename="test-srs.json")

    assert srs_filepath.exists() is False


def test_post_marc_to_srs():
    assert post_marc_to_srs
