import pydantic
import pymarc
import pytest

from unittest.mock import MagicMock

from libsys_airflow.plugins.data_exports.marc.transforms import remove_marc_fields

from libsys_airflow.plugins.data_exports.marc import transformer as marc_transformer


class MockSQLOperator(pydantic.BaseModel):
    task_id: str
    conn_id: str
    database: str
    sql: str

    def execute(self, context):
        mock_result = mock_sql_query_result(self.sql)
        return mock_result


holdings_multiple_items = [
    (
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'instanceId': 'e1797b62-a8b1-5f3d-8e85-934d58bd9395',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
            'effectiveLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
            'callNumberTypeId': '95467209-6d7b-468b-94df-0f5d7ad2747d',
            'callNumber': 'TA357.5 .T87 F74',
        },
    ),
]

holdings_no_items = [
    (
        {
            'id': '194f153f-3f76-5383-b18c-18d67dc5ffa8',
            'instanceId': 'c77d294c-4d83-4fe0-87b1-f94a845c0d49',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'callNumberTypeId': '054d460d-d6b9-4469-9e37-7a78a2266655',
            'callNumber': "QA 124378",
            'permanentLocationId': 'b0a1a8c3-cc9a-487c-a2ed-308fc3a49a91',
        },
    ),
]


multiple_items = [
    (
        {
            'id': 'f34165a0-c41f-59fb-b5a4-f95018303259',
            'materialTypeId': 'd934e614-215d-4667-b231-aed97887f289',
            'numberOfPieces': '1',
            'enumeration': 'V.17 1983',
            'effectiveLocationId': 'bffa197c-a6db-446c-96f7-e1fd37a8842e',
        },
    ),
    (
        {
            'id': 'f341a6dd-2c08-575b-9511-8677fc0229b5',
            'materialTypeId': 'd934e614-215d-4667-b231-aed97887f289',
            'enumeration': 'eV.17 1983',
            'effectiveLocationId': 'b0a1a8c3-cc9a-487c-a2ed-308fc3a49a91',
        },
    ),
]

single_holdings = [
    (
        {
            'id': '10be3fec-48ea-5099-9d5f-ab4875c62481',
            'holdingsTypeId': '03c9c400-b9e3-4a07-ac0e-05ab470233ed',
            'instanceId': '5face3a3-9804-5034-aa02-1eb5db0c191c',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
            'effectiveLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
            'callNumberTypeId': '95467209-6d7b-468b-94df-0f5d7ad2747d',
            'callNumber': 'PQ8098.3.E4 A7',
            'sourceId': 'f32d531e-df79-46b3-8932-cdd35f7a2264',
        },
    ),
]


single_item = [
    (
        {
            'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c',
            'materialTypeId': '1a54b431-2e4f-452d-9cae-9cee66c9a892',
            'numberOfPieces': '1',
            'enumeration': '1989',
            'effectiveLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        },
    ),
]


def mock_sql_query_result(*args):
    # Holdings
    result = [
        ({},),
    ]
    if args[0].endswith("'a75a9e59-8e9a-55cd-8414-f71c1194493b'"):
        result = {"permanentLocationId": "148e598c-bb58-4e6d-b313-4933e6a4534c"}
    if args[0].endswith("'2aa4c0b3-4db6-5c71-a4e2-7fdc672b6b94'"):
        result = {"permanentLocationId": "0edeef57-074a-4f07-aee2-9f09d55e65c3"}
    if args[0].endswith("'5face3a3-9804-5034-aa02-1eb5db0c191c'"):
        result = single_holdings
    if args[0].endswith("'8b373183-2b6f-5a6b-82ab-5f4e6e70d0f8'"):
        result = {"permanentLocationId": "c9cef3c6-5874-4bae-b90b-2e1d1f4674db"}
    if args[0].endswith("'8e9eb01b-1249-5ef8-b9ea-e16496ca64cc'"):
        result = {"permanentLocationId": "46eb9191-1f6f-44ba-a67c-610f868dd429"}
    if args[0].endswith("'e1797b62-a8b1-5f3d-8e85-934d58bd9395'"):
        result = holdings_multiple_items
    if args[0].endswith("'c77d294c-4d83-4fe0-87b1-f94a845c0d49'"):
        result = holdings_no_items
    # Items
    if args[0].endswith("'10be3fec-48ea-5099-9d5f-ab4875c62481'"):
        result = single_item
    if args[0].endswith("'3bb4a439-842e-5c8d-b86c-eaad46b6a316'"):
        result = multiple_items

    return result


@pytest.fixture
def mock_folio_client():
    mock_call_number_types = [
        {
            'id': '95467209-6d7b-468b-94df-0f5d7ad2747d',
            'name': 'Library of Congress classification',
        },
        {
            'id': '054d460d-d6b9-4469-9e37-7a78a2266655',
            'name': 'National Library of Medicine classification',
        },
    ]
    mock_holdings_types = [
        {
            'id': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'name': 'Electronic',
        },
        {'id': '03c9c400-b9e3-4a07-ac0e-05ab470233ed', 'name': 'Monograph'},
    ]
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
            'id': 'b0a1a8c3-cc9a-487c-a2ed-308fc3a49a91',
            'name': 'SUL Electronic',
            'code': 'SUL-ELECTRONIC',
            'libraryId': 'c1a86906-ced0-46cb-8f5b-8cef542bdd00',
        },
    ]
    mock_material_types = {
        "mtypes": [
            {"id": "d934e614-215d-4667-b231-aed97887f289", "name": "periodical"},
            {"id": "1a54b431-2e4f-452d-9cae-9cee66c9a892", "name": "book"},
        ]
    }

    mock_client = MagicMock()
    mock_client.call_number_types = mock_call_number_types
    mock_client.holdings_types = mock_holdings_types
    mock_client.locations = mock_locations
    mock_client.folio_get = lambda *args: mock_material_types
    return mock_client


@pytest.fixture
def mock_get_current_context(mocker):
    context = mocker.stub(name="context")
    context.get = lambda arg: {}
    return context


def test_add_holdings_items_single_999(
    mocker, tmp_path, mock_folio_client, mock_get_current_context
):
    mocker.patch.object(
        marc_transformer, "get_current_context", mock_get_current_context
    )
    mocker.patch.object(marc_transformer, "SQLExecuteQueryOperator", MockSQLOperator)
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
                pymarc.Subfield(code='i', value='5face3a3-9804-5034-aa02-1eb5db0c191c')
            ],
        )
    )
    marc_file = tmp_path / "20240228.mrc"
    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    transformer = marc_transformer.Transformer()
    transformer.add_holdings_items(str(marc_file), full_dump=False)

    with marc_file.open('rb') as fo:
        mod_marc_records = [r for r in pymarc.MARCReader(fo)]

    field_999s = mod_marc_records[0].get_fields('999')

    assert len(field_999s) == 2
    assert field_999s[1].get_subfields('a')[0] == 'PQ8098.3.E4 A7 1989'
    assert field_999s[1].get_subfields('e')[0] == 'GRE-FOLIO-FLAT'
    assert field_999s[1].get_subfields('h')[0] == 'Monograph'
    assert field_999s[1].get_subfields('j')[0] == '1'
    assert field_999s[1].get_subfields('l')[0] == 'GRE-FOLIO-FLAT'
    assert field_999s[1].get_subfields('t')[0] == 'book'
    assert field_999s[1].get_subfields('w')[0].startswith("Library of Congress")


def test_add_holdings_items_multiple_999(
    mocker, tmp_path, mock_folio_client, mock_get_current_context
):
    mocker.patch.object(
        marc_transformer, "get_current_context", mock_get_current_context
    )
    mocker.patch.object(marc_transformer, "SQLExecuteQueryOperator", MockSQLOperator)
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

    marc_file = tmp_path / "2024022911.mrc"
    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    transformer = marc_transformer.Transformer()
    transformer.add_holdings_items(str(marc_file), full_dump=False)

    with marc_file.open('rb') as fo:
        mod_marc_records = [r for r in pymarc.MARCReader(fo)]

    field_999s = mod_marc_records[0].get_fields('999')

    assert len(field_999s) == 3
    assert field_999s[1].get_subfields('a')[0] == "TA357.5 .T87 F74 V.17 1983"
    assert field_999s[2].get_subfields('a')[0] == "TA357.5 .T87 F74 eV.17 1983"
    assert field_999s[1].get_subfields('e')[0] == "BUS-NEWS-STKS"
    assert field_999s[2].get_subfields('e')[0] == "SUL-ELECTRONIC"
    # Both Items have the same Holding so subfield l should be the same
    assert field_999s[1].get_subfields('l') == field_999s[2].get_subfields('l')


def test_add_holdings_items_no_items(
    mocker, tmp_path, mock_folio_client, mock_get_current_context
):
    mocker.patch.object(
        marc_transformer, "get_current_context", mock_get_current_context
    )
    mocker.patch.object(marc_transformer, "SQLExecuteQueryOperator", MockSQLOperator)
    mocker.patch(
        "libsys_airflow.plugins.data_exports.marc.transformer.get_current_context",
        return_value=mock_get_current_context,
    )
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
                pymarc.Subfield(code='i', value='c77d294c-4d83-4fe0-87b1-f94a845c0d49')
            ],
        )
    )

    marc_file = tmp_path / "2024022914.mrc"
    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    transformer = marc_transformer.Transformer()
    transformer.add_holdings_items(str(marc_file), full_dump=False)

    with marc_file.open('rb') as fo:
        mod_marc_records = [r for r in pymarc.MARCReader(fo)]

    field_999s = mod_marc_records[0].get_fields('999')

    assert len(field_999s) == 2
    # Item specific subfields are not present
    assert field_999s[1].get_subfields('e', 'j', 't') == []
    assert field_999s[1].get_subfields('a')[0] == "QA 124378"


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

    remove_marc_fields(str(marc_file.absolute()), full_dump=False)

    with marc_file.open('rb') as fo:
        marc_reader = pymarc.MARCReader(fo)
        modified_marc_record = next(marc_reader)

    assert len(modified_marc_record.fields) == 1

    current_fields = [field.tag for field in modified_marc_record.fields]

    assert "598" not in current_fields
    assert "699" not in current_fields
