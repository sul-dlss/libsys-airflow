import logging
import re

import pymarc

logger = logging.getLogger(__name__)

"""
Matches characters outside the XML 1.0 Char production:
#x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
"""
invalid_xml_chars = re.compile(
    "[^\t\n\r\x20-{}{}-{}{}-{}]".format(
        chr(0xD7FF), chr(0xE000), chr(0xFFFD), chr(0x10000), chr(0x10FFFF)
    )
)


def remove_invalid_xml_chars(record: pymarc.Record) -> pymarc.Record:
    """
    Removes characters not allowed in XML 1.0 from control fields and subfield values
    """
    removed = False
    for field in record.fields:
        if field.is_control_field():
            cleaned = invalid_xml_chars.sub("", field.data or "")
            removed = removed or cleaned != field.data
            field.data = cleaned
            continue
        subfields = []
        for subfield in field.subfields:
            cleaned = invalid_xml_chars.sub("", subfield.value)
            removed = removed or cleaned != subfield.value
            subfields.append(pymarc.Subfield(code=subfield.code, value=cleaned))
        field.subfields = subfields

    if removed:
        hrid = record['001'].value() if '001' in record else "no 001"
        logger.warning(f"Removed invalid XML characters from MARC Record {hrid}")

    return record
