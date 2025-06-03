import pymarc
import pytest

from unittest.mock import MagicMock

from libsys_airflow.plugins.data_exports.marc.exports import (
    marc_for_instances,
)

from libsys_airflow.plugins.data_exports.marc.exporter import Exporter


@pytest.fixture
def mock_folio_client():
    def mock_folio_get(*args, **kwargs):
        output = {
            "parsedRecord": {
                "content": {"leader": "01509nam a2200361 a 4500", "fields": None}
            }
        }
        fields = []
        if "4e66ce0d-4a1d-41dc-8b35-0914df20c7fb" in args[0]:
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
        if "fe2e581f-9767-442a-ae3c-a421ac655fe2" in args[0]:
            # This record gets rejected because it is japanese language (not fre or eng) per _check_008 function
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
                        'subfields': [
                            {'6': '110-01'},
                            {'a': '臨時行政改革推進審議会 (Japan)'},
                        ],
                    }
                },
            ]
        output["parsedRecord"]["content"]["fields"] = fields
        return output

    mock_client = MagicMock()
    mock_client.folio_get = mock_folio_get
    return mock_client


@pytest.fixture
def mock_folio_404():
    def mock_folio_get_404(*args, **kwargs):
        raise ValueError(
            "Error retrieving Record by externalId: '4e66ce0d-4a1d-41dc-8b35-0914df20c7fb', response code 404, Not Found"
        )

    mock_404_client = MagicMock()
    mock_404_client.folio_get = mock_folio_get_404
    return mock_404_client


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker):
    def _context():
        context = mocker.stub(name="context")
        context.get = lambda *args: {
            "bucket": "marc-files",
        }
        return context

    monkeypatch.setattr(
        'libsys_airflow.plugins.data_exports.marc.exporter.get_current_context',
        _context,
    )


def setup_test_file_updates(tmp_path):
    instance_file = (
        tmp_path / "data-export-files/pod/instanceids/updates/202402271159.csv"
    )

    instance_file.parent.mkdir(parents=True)

    with instance_file.open("w+") as fo:
        for instance_uuid in [
            "4e66ce0d-4a1d-41dc-8b35-0914df20c7fb",
            "fe2e581f-9767-442a-ae3c-a421ac655fe2",
        ]:
            fo.write(f"{instance_uuid}\n")

    return instance_file


def setup_test_file_deletes(tmp_path):
    instance_file = (
        tmp_path / "data-export-files/pod/instanceids/deletes/202402271159.csv"
    )

    instance_file.parent.mkdir(parents=True)

    with instance_file.open("w+") as fo:
        for instance_uuid in [
            "0234363b-0289-4aa4-ab2f-816055992e68",
            "0617fa00-2fae-4000-ae4b-ab8369d9afcf",
        ]:
            fo.write(f"{instance_uuid}\n")

    return instance_file


def test_retrieve_marc_for_instances(
    mocker, mock_folio_client, mock_get_current_context, tmp_path
):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.exporter.folio_client',
        return_value=mock_folio_client,
    )

    instance_file = setup_test_file_updates(tmp_path)

    exporter = Exporter()
    exporter.retrieve_marc_for_instances(instance_file, kind="updates")

    marc_file = (
        instance_file.parent.parent.parent / "marc-files/updates/202402271159.mrc"
    )

    assert marc_file.exists()

    with marc_file.open("rb") as fo:
        marc_records = [r for r in pymarc.MARCReader(fo)]

    assert len(marc_records) == 1


def test_retrieve_marc_for_instance_404(mocker, mock_folio_404, tmp_path, caplog):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.exporter.folio_client',
        return_value=mock_folio_404,
    )

    instance_file = setup_test_file_updates(tmp_path)

    exporter = Exporter()
    exporter.retrieve_marc_for_instances(instance_file, kind="updates")

    assert "response code 404" in caplog.text


def test_marc_for_instances(
    mocker, tmp_path, mock_folio_client, mock_get_current_context
):
    update_file_path = setup_test_file_updates(tmp_path)
    delete_file_path = setup_test_file_deletes(tmp_path)

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.exporter.folio_client',
        return_value=mock_folio_client,
    )

    instance_files = [str(update_file_path), str(delete_file_path)]

    files = marc_for_instances(instance_files=f"{instance_files}")

    assert files["updates"][0].endswith('202402271159.mrc')
    assert files["deletes"][0].endswith('202402271159.mrc')

    assert not any("updates" in s for s in files["deletes"])


field_035 = pymarc.Field(
    tag='035',
    indicators=[' ', '9'],  # type: ignore
    subfields=[pymarc.Subfield(code='a', value='gls19291491')],
)

field_008 = pymarc.Field(tag='008', data='920218s1990    ja a          000 0 jpn  ')

field_265 = pymarc.Field(
    tag='265',
    indicators=[' ', ' '],  # type: ignore
    subfields=[pymarc.Subfield(code='a', value='Obsolete field')],
)

field_590 = pymarc.Field(
    tag="590",
    indicators=[' ', ' '],  # type: ignore
    subfields=[pymarc.Subfield(code='a', value='MARCit brief record')],
)

field_590_full = pymarc.Field(
    tag="590",
    indicators=[' ', ' '],  # type: ignore
    subfields=[pymarc.Subfield(code='a', value='MARCit')],
)

field_904 = pymarc.Field(
    tag='903',
    indicators=['1', '0'],  # type: ignore
    subfields=[pymarc.Subfield(code='a', value="A locally defined field")],
)

field_915 = pymarc.Field(
    tag="915",
    indicators=['1', '0'],  # type: ignore
    subfields=[
        pymarc.Subfield(code='a', value='NO EXPORT'),
        pymarc.Subfield(code='b', value='FOR SU ONLY'),
    ],
)

field_915_alt = pymarc.Field(
    tag='915',
    indicators=[' ', '1'],  # type: ignore
    subfields=[pymarc.Subfield(code='a', value='NO EXPORT')],
)

field_915_authority = pymarc.Field(
    tag="915",
    indicators=['1', '0'],  # type: ignore
    subfields=[
        pymarc.Subfield(code='a', value='NO EXPORT'),
        pymarc.Subfield(code='b', value='AUTHORITY VENDOR'),
    ],
)


def test_exclude_marc_by_vendor_backstage(mocker):
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    exporter = Exporter()
    marc_record = pymarc.Record()
    marc_record.add_field(field_590, field_590_full, field_915_authority)
    assert exporter.exclude_marc_by_vendor(marc_record, 'backstage')


def test_exclude_marc_by_vendor_gobi(mocker):
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    exporter = Exporter()
    marc_record = pymarc.Record()
    marc_record.add_field(field_035)

    assert exporter.exclude_marc_by_vendor(marc_record, 'gobi')

    marc_record = pymarc.Record()
    marc_record.add_field(field_008)

    field008 = marc_record['008'].value()
    assert field008[35:38] == 'jpn'
    assert exporter.exclude_marc_by_vendor(marc_record, 'gobi')


def test_exclude_marc_by_vendor_oclc(mocker):
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    exporter = Exporter()
    marc_record = pymarc.Record()
    marc_record.add_field(field_265, field_590, field_915)

    assert exporter.exclude_marc_by_vendor(marc_record, 'oclc')


def test_exclude_marc_by_vendor_pod(mocker):
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    exporter = Exporter()
    marc_record = pymarc.Record()
    marc_record.add_field(field_590, field_904, field_915)

    assert exporter.exclude_marc_by_vendor(marc_record, 'pod')


def test_exclude_marc_by_vendor_sharevde(mocker):
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    exporter = Exporter()
    marc_record = pymarc.Record()
    marc_record.add_field(field_590, field_915)

    assert exporter.exclude_marc_by_vendor(marc_record, 'sharevde')
