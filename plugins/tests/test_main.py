import pytest  # noqa

from plugins.folio.main import _extract_dag_run_id


def test_extract_dag_run_id_reports(tmp_path):
    report = (
        tmp_path
        / "transformation_report_instances_manual__2022-02-14T23:27:04.675755+00:00_bibs.md"  # noqa
    )
    dag_id = _extract_dag_run_id(report)
    assert dag_id == "manual__2022-02-14T23:27:04.675755+00:00"


def test_extract_dag_run_id_mrc(tmp_path):
    error_marc = (
        tmp_path
        / "failed_bib_records_manual__2022-02-14T23:36:51.056707+00:00.mrc"  # noqa
    )
    dag_id = _extract_dag_run_id(error_marc)
    assert dag_id == "manual__2022-02-14T23:36:51.056707+00:00"


def test_extract_run_id_data_issues(tmp_path):
    data_issues = (
        tmp_path
        / "data_issues_log_instances_scheduled__2022-03-09T22:30:49.801082+00:00_bibs-transformer.tsv"  # noqa
    )
    dag_id = _extract_dag_run_id(data_issues)
    assert dag_id == "scheduled__2022-03-09T22:30:49.801082+00:00"


def test_extract_dag_run_id_unknown(tmp_path):
    unknown_file = tmp_path / "sample.txt"
    dag_id = _extract_dag_run_id(unknown_file)
    assert dag_id is None
