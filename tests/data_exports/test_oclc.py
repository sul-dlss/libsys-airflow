import pymarc
import pytest

from unittest.mock import MagicMock

from libsys_airflow.plugins.data_exports.marc.oclc import OCLCTransformer


@pytest.fixture
def sample_marc_records():
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='035',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)34567')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='e1797b62-a8b1-5f3d-8e85-934d58bd9395')
            ],
        ),
    )

    new_record = pymarc.Record()
    new_record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='c5289a13-9e89-4f82-9a86-2e892d6deb9d')
            ],
        )
    )

    return [record, new_record]


@pytest.fixture
def mock_folio_client():
    def mock_folio_get(*args, **kwargs):
        if args[0].endswith("e1797b62-a8b1-5f3d-8e85-934d58bd9395)") or args[
            0
        ].endswith("c5289a13-9e89-4f82-9a86-2e892d6deb9d)"):
            return {
                "holdingsRecords": [
                    {"permanentLocationId": "a8676073-7520-4f26-8573-55976301ab5d"}
                ]
            }
        # Hoover
        if args[0].endswith("faac8aec-5b66-4908-bd56-b0ae9194a546)"):
            return {
                "holdingsRecords": [
                    {"permanentLocationId": "c9cef3c6-5874-4bae-b90b-2e1d1f4674db"}
                ]
            }
        if args[0].endswith("769c09a4-f66f-416d-9326-36f9f368f2e4)"):
            return {
                "holdingsRecords": [
                    {"permanentLocationId": "bffa197c-a6db-446c-96f7-e1fd37a8842e"}
                ]
            }
        # Lane
        if args[0].endswith("9f16e1b3-64e7-4578-9106-c01c2b52c8fe)"):
            return {
                "holdingsRecords": [
                    {"permanentLocationId": "5ffcbe91-775f-484e-91a2-e5473ff6c915"}
                ]
            }
        # Law
        if args[0].endswith("7cf53d65-973b-441c-a7c4-f66467ee077f)"):
            return {
                "holdingsRecords": [
                    {"permanentLocationId": "9f015af5-208c-49b3-9a43-4f079fb4f699"}
                ]
            }
        # No Library
        if args[0].endswith("3256092d-0d78-43e4-a00d-ace904473677)"):
            return {
                "holdingsRecords": [
                    {"permanentLocationId": "11111-2222-333-444-555555"}
                ]
            }
        # Library Locations
        if args[0].startswith("/location-units/libraries"):
            return {
                "loclibs": [
                    {"id": 'f5c58187-3db6-4bda-b1bf-e5f0717e2149', "code": 'BUSINESS'},
                    {"id": "f6b5519e-88d9-413e-924d-9ed96255f72e", "code": "GREEN"},
                    {"id": "ffe6ea8e-1e14-482f-b3e9-66e05efb04dd", "code": "HILA"},
                    {"id": "5b2c8449-eed6-4bd3-bcef-af1e5a225400", "code": "LANE"},
                    {"id": "7e4c05e3-1ce6-427d-b9ce-03464245cd78", "code": "LAW"},
                    {"id": 'c1a86906-ced0-46cb-8f5b-8cef542bdd00', "code": 'SUL'},
                ]
            }

    mock_locations = [
        {
            'id': 'bffa197c-a6db-446c-96f7-e1fd37a8842e',
            'name': 'Business Newspaper Stacks',
            'code': 'BUS-NEWS-STKS',
            'libraryId': 'f5c58187-3db6-4bda-b1bf-e5f0717e2149',
        },
        {
            'id': 'a8676073-7520-4f26-8573-55976301ab5d',
            'name': 'Green Flat Folios',
            'code': 'GRE-FOLIO-FLAT',
            'libraryId': 'f6b5519e-88d9-413e-924d-9ed96255f72e',
        },
        {
            'id': 'c9cef3c6-5874-4bae-b90b-2e1d1f4674db',
            'name': 'HILA Stacks',
            'code': 'HILA-STACKS',
            'libraryId': 'ffe6ea8e-1e14-482f-b3e9-66e05efb04dd',
        },
        {
            'id': '5ffcbe91-775f-484e-91a2-e5473ff6c915',
            'name': 'Lane New Books',
            'code': 'LANE-NEWB',
            'libraryId': '5b2c8449-eed6-4bd3-bcef-af1e5a225400',
        },
        {
            'id': '9f015af5-208c-49b3-9a43-4f079fb4f699',
            'name': 'Law Shadow Order',
            'code': 'LAW-SHADOW-ORD',
            'libraryId': '7e4c05e3-1ce6-427d-b9ce-03464245cd78',
        },
        {
            'id': 'b0a1a8c3-cc9a-487c-a2ed-308fc3a49a91',
            'name': 'SUL Electronic',
            'code': 'SUL-ELECTRONIC',
            'libraryId': 'c1a86906-ced0-46cb-8f5b-8cef542bdd00',
        },
    ]
    mock_client = MagicMock()
    mock_client.folio_get = mock_folio_get
    mock_client.locations = mock_locations
    return mock_client


def test_determine_campus_code(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='e1797b62-a8b1-5f3d-8e85-934d58bd9395')
            ],
        )
    )

    oclc_transformer = OCLCTransformer()
    default_code = oclc_transformer.determine_campus_code(record)

    assert default_code == ["STF"]  # For SUL libraries


def test_determine_campus_code_business(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='769c09a4-f66f-416d-9326-36f9f368f2e4')
            ],
        )
    )

    oclc_transformer = OCLCTransformer()
    biz_code = oclc_transformer.determine_campus_code(record)
    assert biz_code == ["S7Z"]


def test_determine_campus_code_hoover(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='faac8aec-5b66-4908-bd56-b0ae9194a546')
            ],
        )
    )

    oclc_transformer = OCLCTransformer()
    hoover_code = oclc_transformer.determine_campus_code(record)

    assert hoover_code == ["HIN"]


def test_determine_campus_code_lane(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='9f16e1b3-64e7-4578-9106-c01c2b52c8fe')
            ],
        )
    )

    oclc_transformer = OCLCTransformer()
    lane_code = oclc_transformer.determine_campus_code(record)

    assert lane_code == ["CASUM"]


def test_determine_campus_code_law(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='7cf53d65-973b-441c-a7c4-f66467ee077f')
            ],
        )
    )

    oclc_transformer = OCLCTransformer()
    law_code = oclc_transformer.determine_campus_code(record)

    assert law_code == ["RCJ"]


def test_determine_campus_code_no_library(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='3256092d-0d78-43e4-a00d-ace904473677')
            ],
        )
    )
    oclc_transformer = OCLCTransformer()
    codes = oclc_transformer.determine_campus_code(record)
    assert codes == []


def test_oclc_division(mocker, tmp_path, mock_folio_client, sample_marc_records):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / "2024030510.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in sample_marc_records:
            marc_writer.write(record)

    oclc_transformer = OCLCTransformer()
    oclc_transformer.divide(marc_file=str(marc_file.absolute()))

    assert len(oclc_transformer.libraries["STF"]["holdings"]) == 1
    assert len(oclc_transformer.libraries["STF"]["marc"]) == 1


def test_save(mocker, mock_folio_client, sample_marc_records, tmp_path):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / "202401213.mrc"

    hila_record = pymarc.Record()
    hila_record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='faac8aec-5b66-4908-bd56-b0ae9194a546')
            ],
        )
    )

    duplicate_oclc_codes_record = pymarc.Record()
    duplicate_oclc_codes_record.add_field(
        pymarc.Field(
            tag='035',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)123489')],
        ),
        pymarc.Field(
            tag='035',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC-M)445689')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='7cf53d65-973b-441c-a7c4-f66467ee077f')
            ],
        ),
    )

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(hila_record)
        marc_writer.write(duplicate_oclc_codes_record)
        for record in sample_marc_records:
            marc_writer.write(record)

    oclc_transformer = OCLCTransformer()
    oclc_transformer.divide(marc_file=str(marc_file.absolute()))

    oclc_transformer.save()

    assert (marc_file.parent / "202401213-STF-new.mrc").exists()
    assert (marc_file.parent / "202401213-STF-update.mrc").exists()
    assert (marc_file.parent / "202401213-HIN-new.mrc").exists()

    assert oclc_transformer.staff_notices[0] == (
        '7cf53d65-973b-441c-a7c4-f66467ee077f',
        'RCJ',
        ['(OCoLC)123489', '(OCoLC-M)445689'],
    )
