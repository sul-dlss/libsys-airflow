import pytest  # noqa
import shutil
import os
import pathlib

import pymarc
from pydantic import ValidationError

from libsys_airflow.plugins.vendor.marc import (
    process_marc,
    batch,
    _to_change_fields_models,
    _to_add_fields_models,
    _has_matching_field,
    MarcField,
    MarcSubfield,
)


@pytest.fixture
def marc_path(tmp_path):
    dest_filepath = os.path.join(tmp_path, "3820230411.mrc")
    shutil.copyfile("tests/vendor/0720230118.mrc", dest_filepath)
    return pathlib.Path(dest_filepath)


@pytest.fixture
def marcit_path(tmp_path):
    dest_filepath = os.path.join(tmp_path, "marcit_sample_n.mrc")
    shutil.copyfile("tests/vendor/marcit_sample_n.mrc", dest_filepath)
    return pathlib.Path(dest_filepath)


@pytest.fixture
def marc_record():
    with pathlib.Path("tests/vendor/0720230118.mrc").open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        return next(marc_reader)


def test_filter_fields(tmp_path, marc_path):
    new_marc_filename = process_marc(marc_path, ["981", "983"])["marc_filename"]
    assert new_marc_filename == "3820230411-processed.mrc"

    with (pathlib.Path(tmp_path) / new_marc_filename).open("rb") as fo:
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


def test_move_fields(tmp_path, marc_path):
    change_list = _to_change_fields_models(
        [{"from": "520", "to": "920"}, {"from": "504", "to": "904"}]
    )
    new_marc_filename = process_marc(marc_path, change_fields=change_list)[
        "marc_filename"
    ]

    with (pathlib.Path(tmp_path) / new_marc_filename).open("rb") as fo:
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


def test_add_fields(tmp_path, marc_path):
    add_list = _to_add_fields_models(
        [
            {
                "tag": "910",
                "indicator2": 'x',
                "subfields": [{"code": "a", "value": "MarcIt"}],
            }
        ]
    )
    new_marc_filename = process_marc(marc_path, add_fields=add_list)["marc_filename"]

    with (pathlib.Path(tmp_path) / new_marc_filename).open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        for record in marc_reader:
            field = record["910"]
            assert field
            assert field.indicators == [' ', 'x']
            assert field["a"] == "MarcIt"


def test_add_fields_with_unless(tmp_path, marcit_path):
    add_list = _to_add_fields_models(
        [
            {
                "tag": "590",
                "subfields": [{"code": "a", "value": "MARCit brief record"}],
                "unless": {
                    "tag": "035",
                    "subfields": [{"code": "a", "value": "OCoLC"}],
                },
            }
        ]
    )
    new_marc_filename = process_marc(marcit_path, add_fields=add_list)["marc_filename"]

    with (pathlib.Path(tmp_path) / new_marc_filename).open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        for record in marc_reader:
            field035 = record["035"]
            field590 = record["590"]
            assert field035 or field590
            if field035 and field035["a"] == "OCoLC":
                assert not field590
            if not field035 or field035["a"] != "OCoLC":
                assert field590
            if field590:
                assert field590["a"] == "MARCit brief record"


def test_bad_check_fields():
    with pytest.raises(ValidationError):
        _to_change_fields_models([{"from": "520"}, {"from": "504", "to": "904"}])


def test_has_no_matching_field(marc_record):
    marcField = MarcField(tag="246", subfields=[])
    assert not _has_matching_field(marc_record, marcField)

    marcField = MarcField(tag="245", indicator1="x", subfields=[])
    assert not _has_matching_field(marc_record, marcField)

    marcField = MarcField(tag="245", indicator1="1", indicator2="x", subfields=[])
    assert not _has_matching_field(marc_record, marcField)

    marcField = MarcField(
        tag="245",
        indicator1="1",
        indicator2="0",
        subfields=[MarcSubfield(code="a", value="xDiminuendo /")],
    )
    assert not _has_matching_field(marc_record, marcField)

    marcField = MarcField(
        tag="245",
        indicator1="1",
        indicator2="0",
        subfields=[
            MarcSubfield(code="a", value="Diminuendo /"),
            MarcSubfield(code="b", value="test"),
        ],
    )
    assert not _has_matching_field(marc_record, marcField)


def test_has_matching_field(marc_record):
    marcField = MarcField(tag="245", subfields=[])
    assert _has_matching_field(marc_record, marcField)

    marcField = MarcField(tag="245", indicator1="1", subfields=[])
    assert _has_matching_field(marc_record, marcField)

    marcField = MarcField(tag="245", indicator1="1", indicator2="0", subfields=[])
    assert _has_matching_field(marc_record, marcField)

    marcField = MarcField(
        tag="245",
        indicator1="1",
        indicator2="0",
        subfields=[MarcSubfield(code="a", value="Diminuendo /")],
    )
    assert _has_matching_field(marc_record, marcField)

    marcField = MarcField(
        tag="245",
        indicator1="1",
        indicator2="0",
        subfields=[
            MarcSubfield(code="a", value="Diminuendo /"),
            MarcSubfield(code="c", value="Katrinka Moore."),
        ],
    )
    assert _has_matching_field(marc_record, marcField)
