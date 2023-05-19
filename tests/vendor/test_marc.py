import pytest  # noqa
import shutil
import os
import pathlib

import pymarc

from libsys_airflow.plugins.vendor.marc import process_marc, batch, _check_change_fields


@pytest.fixture
def marc_path(tmp_path):
    dest_filepath = os.path.join(tmp_path, "3820230411.mrc")
    shutil.copyfile("tests/vendor/0720230118.mrc", dest_filepath)
    return pathlib.Path(dest_filepath)


def test_filter_fields(marc_path):
    process_marc(marc_path, ["981", "983"])

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
    process_marc(marc_path, change_fields=change_list)

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


def test_bad_check_fields(bad_change_list):
    with pytest.raises(KeyError):
        _check_change_fields(bad_change_list)


def test_check_fields(change_list):
    _check_change_fields(change_list)
