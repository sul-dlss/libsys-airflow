import pathlib

import pymarc
import pytest

from unittest.mock import MagicMock

from libsys_airflow.plugins.data_exports.marc.exports import _exclude_marc_by_vendor, fetch_marc


@pytest.fixture
def mock_folio_client():
    def mock_folio_get(*args, **kwargs):
        output = {
            "parsedRecord": {
                "content": {"leader": "01509nam a2200361 a 4500", "fields": None}
            }
        }
        fields = []
        if args[0].endswith("4e66ce0d-4a1d-41dc-8b35-0914df20c7fb"):
            fields = [
                {'001': 'a4293534'},
                {
                    '100': {
                        'ind1': '1',
                        'ind2': ' ',
                        'subfields': [
                            {'a': 'Marques, Ruy João.'},
                            {'0': '(SIRSI)69071'},
                        ],
                    }
                },
                {
                    '245': {
                        'ind1': '1',
                        'ind2': '0',
                        'subfields': [
                            {
                                'a': '"Casa grande & senzala," Gilberto Freyre e medicina /'
                            },
                            {'c': 'Ruy João Marques.'},
                        ],
                    }
                },
                {
                    "590": {
                        'ind1': ' ',
                        'ind2': ' ',
                        'subfields': [{'a': 'MARCit brief record'}],
                    }
                },
                {
                    '650': {
                        'ind1': ' ',
                        'ind2': '0',
                        'subfields': [
                            {'a': 'Social medicine'},
                            {'z': 'Brazil.'},
                            {'0': '(SIRSI)2545689'},
                        ],
                    }
                },
            ]
        if args[0].endswith("fe2e581f-9767-442a-ae3c-a421ac655fe2"):
            fields = [
                {'001': 'a4232294'},
                {'008': '920218s1990    ja a          000 0 jpn  '},
                {
                    '245': {
                        'ind1': '1',
                        'ind2': '0',
                        'subfields': [
                            {'6': '880-02'},
                            {'a': '"Chihō no jidai" no shintenkai :'},
                            {'b': 'Shin Gyōkakushin tōshin /'},
                            {
                                'c': 'kanshū Rinji Gyōsei Kaikaku Suishin Shingikai Jimushitsu.'
                            },
                        ],
                    }
                },
                {
                    '880': {
                        'ind1': '2',
                        'ind2': ' ',
                        'subfields': [{'6': '110-01'}, {'a': '臨時行政改革推進審議会 (Japan)'}],
                    }
                },
            ]
        output["parsedRecord"]["content"]["fields"] = fields
        return output

    mock_client = MagicMock()
    mock_client.folio_get = mock_folio_get
    return mock_client


def test_fetch_marc(tmp_path, mock_folio_client):
    instance_file = tmp_path / "pod/Instanceids/2024022711.csv"

    instance_file.parent.mkdir(parents=True)

    with instance_file.open("w+") as fo:
        for instance_uuid in [
            "4e66ce0d-4a1d-41dc-8b35-0914df20c7fb",
            "fe2e581f-9767-442a-ae3c-a421ac655fe2",
        ]:
            fo.write(f"{instance_uuid}\n")

    returned_marc_path = fetch_marc(
        instance_file=str(instance_file), folio_client=mock_folio_client
    )

    marc_file = pathlib.Path(returned_marc_path)

    assert marc_file.exists()

    with marc_file.open("rb") as fo:
        marc_records = [r for r in pymarc.MARCReader(fo)]

    assert len(marc_records) == 1


def test_fetch_marc_missing_instance_file():
    with pytest.raises(ValueError, match="2024022409.csv does not exist"):
        fetch_marc(instance_file="2024022409.csv")


field_001 = pymarc.Field(tag='001', data='gls')

field_008 = pymarc.Field(tag='008', data='920218s1990    ja a          000 0 jpn  ')

field_590 = pymarc.Field(
    tag="590",
    indicators=[' ', ' '],
    subfields=[pymarc.Subfield(code='a', value='MARCit brief record')],
)

field_915 = pymarc.Field(
    tag="915",
    indicators=['1', '0'],
    subfields=[
        pymarc.Subfield(code='a', value='NO EXPORT'),
        pymarc.Subfield(code='b', value='FOR SU ONLY'),
    ],
)

field_915_alt = pymarc.Field(
    tag='915',
    indicators=[' ', '1'],
    subfields=[pymarc.Subfield(code='a', value='NO EXPORT')],
)


def test_exclude_marc_by_vendor_gobi():
    marc_record = pymarc.Record()
    marc_record.add_field(field_001, field_008)

    assert _exclude_marc_by_vendor(marc_record, 'gobi')

    marc_record = pymarc.Record()
    marc_record.add_field(field_008)

    assert _exclude_marc_by_vendor(marc_record, 'gobi')


def test_exclude_marc_by_vendor_oclc():
    marc_record = pymarc.Record()
    marc_record.add_field(field_590, field_915)

    assert _exclude_marc_by_vendor(marc_record, 'oclc')


def test_exclude_marc_by_vendor_pod():
    marc_record = pymarc.Record()
    marc_record.add_field(field_590, field_915)

    assert _exclude_marc_by_vendor(marc_record, 'pod')


def test_exclude_marc_by_vendor_sharevde():
    marc_record = pymarc.Record()
    marc_record.add_field(field_590, field_915)

    assert _exclude_marc_by_vendor(marc_record, 'sharevde')
