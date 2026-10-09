import io
import pathlib

from unittest.mock import MagicMock

import pymarc
import pytest

from s3path import S3Path

from libsys_airflow.plugins.data_exports.marc.marc_io import (
    CleanXMLWriter,
    is_marc_xml,
    marc_writer,
    overwrite_marc_file,
    read_marc,
    record_hrid,
    remove_invalid_xml_chars,
)
from libsys_airflow.plugins.data_exports.marc.transforms import marc_clean_serialize


def record_with_invalid_chars():
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(tag='001', data='a2545001\x0b'),
        pymarc.Field(
            tag='245',
            indicators=['1', '0'],
            subfields=[
                pymarc.Subfield(code='a', value='A Title\x1f with \x1bcontrols'),
                pymarc.Subfield(code='c', value='Café ☃ 𝄞'),
            ],
        ),
    )
    return record


def write_xml(record):
    fo = io.BytesIO()
    writer = pymarc.XMLWriter(fo)
    writer.write(record)
    writer.close(close_fh=False)
    fo.seek(0)
    return fo


def test_invalid_xml_chars_break_parsing():
    with pytest.raises(Exception, match="not well-formed"):
        pymarc.parse_xml_to_array(write_xml(record_with_invalid_chars()))


def test_remove_invalid_xml_chars(caplog):
    record = remove_invalid_xml_chars(record_with_invalid_chars())

    assert "Removed invalid XML characters from MARC Record a2545001" in caplog.text

    records = pymarc.parse_xml_to_array(write_xml(record))

    assert records[0]['001'].value() == 'a2545001'
    assert records[0]['245']['a'] == 'A Title with controls'
    assert records[0]['245']['c'] == 'Café ☃ 𝄞'


def test_remove_invalid_xml_chars_no_change(caplog):
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(tag='001', data='a123'),
        pymarc.Field(
            tag='245',
            indicators=['0', '0'],
            subfields=[pymarc.Subfield(code='a', value='Tab\tand newline\n')],
        ),
    )

    remove_invalid_xml_chars(record)

    assert record['245']['a'] == 'Tab\tand newline\n'
    assert "Removed invalid XML characters" not in caplog.text


def test_record_hrid(caplog):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag='001', data='a123'))
    assert record_hrid(record) == 'a123'

    no_001 = pymarc.Record()
    no_001.add_field(
        pymarc.Field(
            tag='245',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='A Title\x0b')],
        )
    )
    assert record_hrid(no_001) == ''

    remove_invalid_xml_chars(no_001)
    assert "Removed invalid XML characters from MARC Record no 001" in caplog.text


def test_is_marc_xml():
    assert is_marc_xml(pathlib.Path("0_5000.xml"))
    assert not is_marc_xml(pathlib.Path("0_5000.mrc"))


@pytest.mark.parametrize("suffix", [".xml", ".mrc"])
def test_marc_writer_read_marc_by_suffix(tmp_path, suffix):
    marc_path = tmp_path / f"202610081000{suffix}"

    with marc_path.open("wb") as fo:
        writer = marc_writer(fo, marc_path)
        writer.write(record_with_invalid_chars())
        writer.close(close_fh=False)

    assert isinstance(writer, CleanXMLWriter) is (suffix == ".xml")

    records = read_marc(marc_path)

    assert len(records) == 1
    assert records[0]['245']['c'] == 'Café ☃ 𝄞'
    if suffix == ".xml":
        assert records[0]['245']['a'] == 'A Title with controls'


def test_overwrite_marc_file(tmp_path):
    marc_path = tmp_path / "202610081000.xml"
    marc_path.write_bytes(b"original")

    with overwrite_marc_file(marc_path) as fo:
        fo.write(b"new")
        # the original is untouched until the write completes
        assert marc_path.read_bytes() == b"original"

    assert marc_path.read_bytes() == b"new"
    assert not (tmp_path / "202610081000.xml.tmp").exists()


def test_overwrite_marc_file_failed_write(tmp_path):
    marc_path = tmp_path / "202610081000.xml"
    marc_path.write_bytes(b"original")

    with pytest.raises(RuntimeError):
        with overwrite_marc_file(marc_path) as fo:
            fo.write(b"partial")
            raise RuntimeError("worker died")

    assert marc_path.read_bytes() == b"original"
    assert not (tmp_path / "202610081000.xml.tmp").exists()


def test_overwrite_marc_file_s3():
    marc_path = MagicMock(spec=S3Path)

    with overwrite_marc_file(marc_path) as fo:
        fo.write(b"new")

    marc_path.open.assert_called_once_with("wb")
    marc_path.with_name.assert_not_called()


def test_marc_clean_serialize_failed_write_keeps_file(mocker, tmp_path):
    marc_path = tmp_path / "202610081000.xml"
    with marc_path.open("wb") as fo:
        writer = pymarc.XMLWriter(fo)
        for hrid in ["a1", "a2", "a3"]:
            record = pymarc.Record()
            record.add_field(pymarc.Field(tag='001', data=hrid))
            writer.write(record)
        writer.close(close_fh=False)
    original = marc_path.read_bytes()

    record_to_xml_node = pymarc.record_to_xml_node
    calls = []

    def fail_on_second_record(record, **kwargs):
        calls.append(record)
        if len(calls) == 2:
            raise RuntimeError("worker died")
        return record_to_xml_node(record, **kwargs)

    mocker.patch(
        "libsys_airflow.plugins.data_exports.marc.transforms.pymarc.record_to_xml_node",
        side_effect=fail_on_second_record,
    )

    with pytest.raises(RuntimeError):
        marc_clean_serialize(str(marc_path), full_dump=False, exclude_tags=True)

    assert marc_path.read_bytes() == original
    assert [r['001'].value() for r in read_marc(marc_path)] == ["a1", "a2", "a3"]
