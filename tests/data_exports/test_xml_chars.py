import io

import pymarc
import pytest

from libsys_airflow.plugins.data_exports.marc.xml_chars import (
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
