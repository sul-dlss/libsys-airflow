import httpx
import pymarc
import pytest

from unittest.mock import MagicMock

from libsys_airflow.plugins.data_exports.marc.oclc import (
    OCLCTransformer,
    archive_instanceid_csv,
    get_record_id,
)


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

    no_999_record = pymarc.Record()
    no_999_record.add_field(pymarc.Field(tag='001', data='123456abc'))

    return [record, new_record, no_999_record]


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
        # Raises 400 Error for missing instanceId
        if args[0].endswith("instanceId==)"):
            raise httpx.HTTPStatusError(
                f"400 Bad Request for url '{args[0]}'",
                request=httpx.Request('GET', args[0]),
                response=httpx.Response(400),
            )

        # Campus locations
        if args[0].startswith("/location-units/campuses"):
            return {
                "loccamps": [
                    {
                        "id": "b89563c5-cb66-4de7-b63c-ca4d82e9d856",
                        "name": "Graduate School of Business",
                        "code": "GSB",
                    },
                    {
                        "id": "be6468b8-ed88-4876-93fe-5bdac764959c",
                        "name": "Hoover Institution",
                        "code": "HOOVER",
                    },
                    {
                        "id": "7003123d-ef65-45f6-b469-d2b9839e1bb3",
                        "name": "Law School",
                        "code": "LAW",
                    },
                    {
                        "id": "40b76104-95ea-4360-a2be-5fd887222e2d",
                        "name": "Medical Center",
                        "code": "MED",
                    },
                    {
                        "id": "c365047a-51f2-45ce-8601-e421ca3615c5",
                        "name": "Stanford Libraries",
                        "code": "SUL",
                    },
                ]
            }
        # Material types
        if args[0].startswith("/material-types"):
            return {
                "mtypes": [
                    {
                        "id": "d934e614-215d-4667-b231-aed97887f289",
                        "name": "periodical",
                    },
                    {"id": "1a54b431-2e4f-452d-9cae-9cee66c9a892", "name": "book"},
                ]
            }
        # Campuses
        if args[0].startswith("/location-units/campuses"):
            return {
                "loccamps": [
                    {"id": "c365047a-51f2-45ce-8601-e421ca3615c5", "code": "SUL"},
                ],
            }

    mock_locations = [
        {
            'id': 'bffa197c-a6db-446c-96f7-e1fd37a8842e',
            'name': 'Business Newspaper Stacks',
            'code': 'BUS-NEWS-STKS',
            "campusId": "b89563c5-cb66-4de7-b63c-ca4d82e9d856",
            'libraryId': 'f5c58187-3db6-4bda-b1bf-e5f0717e2149',
        },
        {
            'id': 'a8676073-7520-4f26-8573-55976301ab5d',
            'name': 'Green Flat Folios',
            'code': 'GRE-FOLIO-FLAT',
            "campusId": "c365047a-51f2-45ce-8601-e421ca3615c5",
            'libraryId': 'f6b5519e-88d9-413e-924d-9ed96255f72e',
        },
        {
            'id': 'c9cef3c6-5874-4bae-b90b-2e1d1f4674db',
            'name': 'HILA Stacks',
            'code': 'HILA-STACKS',
            "campusId": "be6468b8-ed88-4876-93fe-5bdac764959c",
            'libraryId': 'ffe6ea8e-1e14-482f-b3e9-66e05efb04dd',
        },
        {
            'id': '5ffcbe91-775f-484e-91a2-e5473ff6c915',
            'name': 'Lane New Books',
            'code': 'LANE-NEWB',
            "campusId": "40b76104-95ea-4360-a2be-5fd887222e2d",
            'libraryId': '5b2c8449-eed6-4bd3-bcef-af1e5a225400',
        },
        {
            'id': '9f015af5-208c-49b3-9a43-4f079fb4f699',
            'name': 'Law Shadow Order',
            'code': 'LAW-SHADOW-ORD',
            "campusId": "7003123d-ef65-45f6-b469-d2b9839e1bb3",
            'libraryId': '7e4c05e3-1ce6-427d-b9ce-03464245cd78',
        },
        {
            'id': 'b0a1a8c3-cc9a-487c-a2ed-308fc3a49a91',
            'name': 'SUL Electronic',
            'code': 'SUL-ELECTRONIC',
            "campusId": "c365047a-51f2-45ce-8601-e421ca3615c5",
            'libraryId': 'c1a86906-ced0-46cb-8f5b-8cef542bdd00',
        },
    ]
    mock_client = MagicMock()
    mock_client.folio_get = mock_folio_get
    mock_client.locations = mock_locations
    return mock_client


def test_archive_instanceid_csv(tmp_path):
    oclc_dir = tmp_path / "oclc"
    oclc_dir.mkdir(parents=True, exist_ok=True)
    instance_ids_dir = oclc_dir / "instanceids"
    transmitted_dir = oclc_dir / "transmitted"

    new_instance_ids = instance_ids_dir / "new/202406171725.csv"
    new_instance_ids.parent.mkdir(parents=True, exist_ok=True)
    new_instance_ids.touch()

    update_instance_ids = instance_ids_dir / "updates/202406171725.csv"
    update_instance_ids.parent.mkdir(parents=True, exist_ok=True)
    update_instance_ids.touch()

    archive_instanceid_csv([str(new_instance_ids), str(update_instance_ids)])

    assert (transmitted_dir / "new/202406171725.csv").exists()
    assert (transmitted_dir / "updates/202406171725.csv").exists()
    assert new_instance_ids.exists() is False
    assert update_instance_ids.exists() is False


def test_determine_campus_code(mocker, mock_folio_client):
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
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
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
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
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
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
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
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
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
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
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
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


def test_determine_campus_code_http_error(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    record = pymarc.Record()
    record.add_field(pymarc.Field(tag='001', data='123456abc'))

    oclc_transformer = OCLCTransformer()
    codes = oclc_transformer.determine_campus_code(record)
    assert codes == []


def test_oclc_division(mocker, tmp_path, mock_folio_client, sample_marc_records):
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
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
    mocker.patch('libsys_airflow.plugins.data_exports.marc.transformer.SQLPool')
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    new_dir = tmp_path / "new"

    new_dir.mkdir(parents=True, exist_ok=True)

    marc_file = new_dir / "202401213.mrc"

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

    assert (marc_file.parent / "202401213-STF.mrc").exists()
    assert (marc_file.parents[1] / "updates/202401213mv-STF.mrc").exists()
    assert (marc_file.parent / "202401213-HIN.mrc").exists()

    assert (
        oclc_transformer.staff_notices[0][0] == '7cf53d65-973b-441c-a7c4-f66467ee077f'
    )
    assert oclc_transformer.staff_notices[0][1] == 'RCJ'
    assert sorted(oclc_transformer.staff_notices[0][2]) == ['123489', '445689']


def test_get_record_id(mocker, mock_folio_client):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    record_oclc_i = pymarc.Record()
    record_oclc_i.add_field(
        pymarc.Field(
            tag="035",
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value="(OCoLC-I)275220973")],
        ),
        pymarc.Field(
            tag="035",
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value="(OCoLC-M)38180605")],
        ),
    )

    oclc_no_i_ids = get_record_id(record_oclc_i)
    assert oclc_no_i_ids == ["38180605"]

    record_dup_and_prefixes = pymarc.Record()
    record_dup_and_prefixes.add_field(
        pymarc.Field(
            tag='035',
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value="(OCoLC)on1427207959")],
        ),
        pymarc.Field(
            tag='035',
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value="(OCoLC-M)1427207959")],
        ),
        pymarc.Field(
            tag='035',
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value="(OCoLC)ocm1427207959")],
        ),
    )

    oclc_ids = get_record_id(record_dup_and_prefixes)

    assert oclc_ids == ["1427207959"]
