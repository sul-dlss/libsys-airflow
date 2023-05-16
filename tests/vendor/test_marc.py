import pytest  # noqa
import shutil
import os
import pathlib
import logging
import filecmp

import pymarc

from libsys_airflow.plugins.vendor.marc import filter_fields, batch, move_fields


@pytest.fixture
def marc_path(tmp_path):
    dest_filepath = os.path.join(tmp_path, "3820230411.mrc")
    shutil.copyfile("tests/vendor/0720230118.mrc", dest_filepath)
    return pathlib.Path(dest_filepath)


def test_filter_fields(marc_path, caplog):
    caplog.set_level(logging.INFO)
    filter_fields(marc_path, ["981", "983"])

    with marc_path.open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        for record in marc_reader:
            assert record.get_fields("981", "983") == []


def test_batch(tmp_path, marc_path):
    assert batch(tmp_path, "3820230411.mrc", 10) == [
        "3820230411_1.mrc",
        "3820230411_2.mrc",
        "3820230411_3.mrc",
    ]

    batch_path = pathlib.Path(tmp_path) / "3820230411_1.mrc"
    count = 0
    with batch_path.open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        for record in marc_reader:
            count += 1
    assert count == 10


@pytest.fixture
def change_list():
    return [{"from": "520", "to": "920"}, {"from": "504", "to": "904"}]


@pytest.fixture
def bad_change_list():
    return [{"from": "520"}, {"from": "504", "to": "904"}]


def test_move_fields(marc_path, change_list):
    move_fields(marc_path, change_list)

    with marc_path.open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        for record in marc_reader:
            assert record.get_fields("520", "504") == []
            if record.title() == "The loneliest whale blues /":
                assert record.get_fields("920")
            if (
                record.title()
                == "The FVN handbook : the principles and practices of the Fierce Vulnerability Network."
            ):
                assert record.get_fields("904")


def test_move_fields_bad_changes(marc_path, bad_change_list, caplog, tmp_path):
    caplog.set_level(logging.INFO)
    original_file = shutil.copyfile(marc_path, os.path.join(tmp_path, "original.mrc"))
    move_fields(marc_path, bad_change_list)

    assert "Not moving fields." in caplog.text
    assert filecmp.cmp(marc_path, original_file)
