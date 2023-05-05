import pytest  # noqa
import shutil
import os
import pathlib
import logging

import pymarc

from libsys_airflow.plugins.vendor.marc import filter_fields, batch


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
