import pytest  # noqa

from plugins.tests.mocks import mock_file_system, mock_dag_run  # noqa

from plugins.folio.apps.folio_migration_view import (
    _get_catkey_range,
    _get_folio_records,
    _get_reports_data_issues,
    _get_source_data,
    FOLIOMigrationReports,
)


def test_get_catkey_range_existing_mrc(mock_file_system):  # noqa

    iteration_dir = mock_file_system[2]

    marc_file = iteration_dir / "source_data/instances/ckeys_00350000_00399999.mrc"
    marc_file.write_text("marc string")

    start, end = _get_catkey_range(iteration_dir)

    assert start == "350,000"
    assert end == "399,999"


def test_get_catkey_range_nonexistant_mrc(mock_file_system):  # noqa

    iteration_dir = mock_file_system[2]

    start, end = _get_catkey_range(iteration_dir)

    assert start is None
    assert end is None


def test_get_catkey_range_nonstandard_name(mock_file_system):  # noqa

    iteration_dir = mock_file_system[2]

    marc_file = iteration_dir / "source_data/instances/ON-ORDER.law.01.mrc"
    marc_file.write_text("marc_string")

    start, end = _get_catkey_range(iteration_dir)

    assert start is None
    assert end is None


def test_get_folio_records(mock_file_system):  # noqa

    iteration_dir = mock_file_system[2]

    (iteration_dir / "results/folio_instances_bibs-transformer.json").write_text(
        """{ id="1234" }"""
    )

    (iteration_dir / "results/folio_holdings_tsv-transformer.json").write_text(
        """{ id="45567" }"""
    )

    (iteration_dir / "results/folio_holdings_electronic-transformer.json").write_text(
        """{ id="49900" }"""
    )

    (iteration_dir / "results/folio_items_transformer.json").write_text(
        """{ id="49900" }"""
    )

    records = _get_folio_records(iteration_dir)

    assert len(records["instances"]) == 1
    assert len(records["holdings"]) == 2
    assert records["items"][0].startswith("folio_items_transformer.json")


def test_get_reports_data_issues(mock_file_system):  # noqa

    iteration_dir = mock_file_system[2]

    (iteration_dir / "reports/report_bibs-transformer.md").write_text("# A Report")

    (iteration_dir / "reports/data_issues_log_tsv-transformer.tsv").write_text(
        """Type\tCatkey\tError\tValue\nRECORD FAILED\ta350008\tCould not map\tSAL3 Stacks missing"""
    )

    reports, data_issues = _get_reports_data_issues(iteration_dir)

    assert reports[0].startswith("report_bibs-transformer.md")
    assert data_issues[0].startswith("data_issues_log_tsv-transformer.tsv")


def test_get_source_data(mock_file_system):  # noqa

    iteration_dir = mock_file_system[2]

    (iteration_dir / "source_data/instances/ckey_003000_005000.mrc").write_text(
        "marc record"
    )

    (iteration_dir / "source_data/holdings/ckey_003000_005000.mhlds.mrc").write_text(
        "mhlds record"
    )

    (iteration_dir / "source_data/items/ckey_003_005.tsv").write_text(
        """CATKEY\tTEMPORARY_LOCATION\tLIBRARY\tBARCODE\na3456\tSTACKS\tGREEN\t3456789"""
    )

    sources = _get_source_data(iteration_dir)

    assert sources["instances"] == ["ckey_003000_005000.mrc"]
    assert len(sources["holdings"]) == 2
    assert len(sources["items"]) == 1


def test_folio_migration_view():
    folio_app = FOLIOMigrationReports()
    assert folio_app
