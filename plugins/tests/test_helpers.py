import pytest  # noqa


from plugins.folio.helpers import (
    archive_artifacts,
    move_marc_files_check_csv,
    process_marc,
    tranform_csv_to_tsv,
)


def test_archive_artifacts():
    assert archive_artifacts


def test_move_marc_files():
    assert move_marc_files_check_csv


def test_process_marc():
    assert process_marc


def test_tranform_csv_to_tsv():
    assert tranform_csv_to_tsv
