import io
import pathlib

import pymarc
import pytest

from libsys_airflow.plugins.data_exports.marc.marc_io import (
    CleanXMLWriter,
    is_marc_xml,
    marc_writer,
    read_marc,
    record_hrid,
    remove_invalid_xml_chars,
)


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
