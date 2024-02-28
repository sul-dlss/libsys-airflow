import pymarc
import pytest

from libsys_airflow.plugins.data_exports.marc.transforms import remove_marc_fields


def test_remove_marc_fields(tmp_path):
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='245',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='A Short Title')],
        ),
        pymarc.Field(
            tag="598",
            indicators=[' ', '1'],
            subfields=[pymarc.Subfield(code='a', value='a30')],
        ),
        pymarc.Field(
            tag="699",
            indicators=['0', '4'],
            subfields=[pymarc.Subfield(code='a', value='see90 8')],
        ),
        pymarc.Field(
            tag="699",
            indicators=['0', '4'],
            subfields=[pymarc.Subfield(code='a', value='see90 9')],
        ),
    )

    marc_file = tmp_path / "20240228.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    remove_marc_fields(str(marc_file.absolute()))

    with marc_file.open('rb') as fo:
        marc_reader = pymarc.MARCReader(fo)
        modified_marc_record = next(marc_reader)

    assert len(modified_marc_record.fields) == 1

    current_fields = [field.tag for field in modified_marc_record.fields]

    assert "598" not in current_fields
    assert "699" not in current_fields
