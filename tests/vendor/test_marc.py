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
    _marc8_to_unicode,
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
def encoding_marc_path(tmp_path):
    dest_filepath = tmp_path / "encoding-samples.mrc"
    shutil.copyfile("tests/vendor/encoding-samples.mrc", dest_filepath)
    return dest_filepath


@pytest.fixture
def marc_record():
    with pathlib.Path("tests/vendor/0720230118.mrc").open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        return next(marc_reader)


def test_filter_fields(tmp_path, marc_path):
    new_marc_filename = process_marc(marc_path, ["981", "983"])["filename"]
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
        [
            {"from": {"tag": "001"}, "to": {"tag": "035", "indicator2": "9"}},
            {"from": {"tag": "520", "indicator1": " "}, "to": {"tag": "920"}},
            {
                "from": {"tag": "504"},
                "to": {"tag": "904", "indicator1": "a", "indicator2": " "},
            },
        ]
    )
    new_marc_filename = process_marc(marc_path, change_fields=change_list)["filename"]

    with (pathlib.Path(tmp_path) / new_marc_filename).open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        for record in marc_reader:
            if record.title == "The loneliest whale blues /":
                field = record.get_fields("035")[0]
                assert field
                assert field.indicator1 == " "
                assert field.indicator2 == "9"
                assert field["a"] == "gls17928831"
                field = record.get_fields("920")[0]
                assert field
                assert field.indicator1 == " "
                assert field.indicator2 == " "
            if (
                record.title
                == "The FVN handbook : the principles and practices of the Fierce Vulnerability Network."
            ):
                field = record.get_fields("904")[0]
                assert field
                assert field.indicator1 == "a"
                assert field.indicator2 == " "


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
    new_marc_filename = process_marc(marc_path, add_fields=add_list)["filename"]

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
                "subfields": [{"code": "a", "value": "MARCit brief record."}],
                "unless": {
                    "tag": "035",
                    "subfields": [{"code": "a", "value": "OCoLC"}],
                },
            }
        ]
    )
    new_marc_filename = process_marc(marcit_path, add_fields=add_list)["filename"]

    with (pathlib.Path(tmp_path) / new_marc_filename).open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        for record in marc_reader:
            field035 = record["035"]
            field590s = record.get_fields("590")
            assert field035 or field590s
            if field035 and "OCoLC" in field035["a"]:
                assert not field590s
            if not field035 or "OCoLC" not in field035["a"]:
                assert field590s
            if field590s:
                assert field590s[0]["a"] == "MARCit brief record."


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


def test_marc8_to_unicode(encoding_marc_path):
    with encoding_marc_path.open("rb") as fo:
        marc_records = [record for record in pymarc.MARCReader(fo, to_unicode=False)]

    mod_record = _marc8_to_unicode(marc_records[0])

    assert mod_record.leader[9] == " "

    # The original 245 subfield 'a' had mixed marc8 and utf-8 characters
    # Spaces added at the end for those tokens that could not be parsed
    assert mod_record.title == "한국환경농학회 워크      "

    assert marc_records[1].leader[9] == "a"
    mod_record2 = _marc8_to_unicode(marc_records[1])

    assert marc_records[1].leader[9] == " "
    # Confirms that other records are encoded correctly
    assert (
        mod_record2.title
        == "Signal. image. architecture. : everything is already an image /"
    )
