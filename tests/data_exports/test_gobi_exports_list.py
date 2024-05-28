import pathlib
import pymarc

from libsys_airflow.plugins.data_exports.marc import gobi as gobi_transformer
from tests.data_exports.test_marc_transformations import mock_folio_client  # noqa


def record(**kwargs):
    isbns = kwargs.get("isbns")
    fields035 = kwargs.get("fields035")
    fields856 = kwargs.get("fields856")
    fields956 = kwargs.get("fields956")

    record = pymarc.Record()
    for field in isbns:
        record.add_field(
            pymarc.Field(
                tag='020',
                indicators=[' ', ' '],
                subfields=[pymarc.Subfield(code='a', value=field)],
            )
        )

    for field in fields035:
        record.add_field(
            pymarc.Field(
                tag='035',
                indicators=[' ', ' '],
                subfields=[pymarc.Subfield(code='a', value=field)],
            )
        )

    for field in fields856:
        record.add_field(
            pymarc.Field(
                tag='856',
                indicators=[' ', ' '],
                subfields=[pymarc.Subfield(code='x', value=field)],
            )
        )

    for field in fields956:
        record.add_field(
            pymarc.Field(
                tag='956',
                indicators=[' ', ' '],
                subfields=[pymarc.Subfield(code='x', value=field)],
            )
        )

    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=[' ', ' '],
            subfields=[
                pymarc.Subfield(code='i', value='5db0204e-0c10-40d7-8d11-26b94177b170')
            ],
        )
    )
    return record


folio_result = {
    "mtypes": [
        {"id": "1a54b431-2e4f-452d-9cae-9cee66c9a892", "name": "book"},
    ],
    "loclibs": [
        {
            "id": "a8676073-7520-4f26-8573-55976301ab5d",
            "campusId": "c365047a-51f2-45ce-8601-e421ca3615c5",
            "code": "GREEN",
        },
    ],
    "loccamps": [
        {"id": "b89563c5-cb66-4de7-b63c-ca4d82e9d856", "code": "GSB"},
        {"id": "c365047a-51f2-45ce-8601-e421ca3615c5", "code": "SUL"},
    ],
}


def test_with_ebook_and_print(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240101"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = [
        {
            'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c',
        },
    ]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890123", "9876543212345"],
                fields035=["notgls12345", "other1value"],
                fields856=["notgobi", "other"],
                fields956=["notsubscribed", "other"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == "1234567890123|print|325099\n"
        assert fo.readline() == "9876543212345|print|325099\n"
        assert fo.readline() == "1234567890123|ebook|325099\n"
        assert fo.readline() == "9876543212345|ebook|325099\n"


def test_with_ebook_only(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240102"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = []  # <-- No items, so no added line for "print"

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890123"],
                fields035=["notgls12345"],
                fields856=["notgobi"],
                fields956=["notsubscribed"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == "1234567890123|ebook|325099\n"


def test_with_no_isbn(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240103.mrc"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = [
        {
            'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c',
        },
    ]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["42"],  # <-- Not a valid ISBN
                fields035=["notgls12345"],
                fields856=["notgobi"],
                fields956=["notsubscribed"],
            )
        )
        marc_writer.write(
            record(
                isbns=[],  # <-- No ISBNs
                fields035=["notgls12345"],
                fields856=["notgobi"],
                fields956=["notsubscribed"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == ""


def test_with_modified_isbn(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240203.mrc"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = [
        {
            'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c',
        },
    ]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890 some text", "9-876543212345"],
                fields035=["notgls12345", "other1value"],
                fields856=["notgobi", "other"],
                fields956=["notsubscribed", "other"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == "1234567890|print|325099\n"
        assert fo.readline() == "9876543212345|print|325099\n"
        assert fo.readline() == "1234567890|ebook|325099\n"
        assert fo.readline() == "9876543212345|ebook|325099\n"


def test_with_print_no_electronic_holding(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240104"

    holdings = [
        {
            'id': 'xxxxxxxx-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': 'xxxxxxxx-5b5e-4cf2-9168-33ced1f95eed',  # <-- does not equal "Electronic", so no added line for "ebook"
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = [{'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c'}]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890123"],
                fields035=["notgls12345"],
                fields856=["notgobi"],
                fields956=["notsubscribed"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == "1234567890123|print|325099\n"
        assert fo.readline() == ""


def test_with_skipped_by_035(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240105"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = [
        {
            'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c',
        }
    ]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890123"],
                fields035=[
                    "gls12345"
                ],  # <-- indicates an existing Gobi record, so no added line for "ebook"
                fields856=["notgobi"],
                fields956=["notsubscribed"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == "1234567890123|print|325099\n"
        assert fo.readline() == ""


def test_with_skipped_by_856(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240106"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = [{'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c'}]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890123"],
                fields035=["notgls12345"],
                fields856=[
                    "gobi",
                    "notgobi",
                ],  # <-- indicates an existing Gobi record, so no added line for "ebook"
                fields956=["notsubscribed"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == "1234567890123|print|325099\n"
        assert fo.readline() == ""


def test_with_skipped_by_956(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240107"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'a8676073-7520-4f26-8573-55976301ab5d',
        }
    ]

    items = [{'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c'}]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890123"],
                fields035=["notgls12345"],
                fields856=["notgobi"],
                fields956=[
                    "subscribed",
                    "notsubscribed",
                ],  # <-- indicates an existing ebook record, so no added line for "ebook"
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == "1234567890123|print|325099\n"
        assert fo.readline() == ""


def test_with_non_sul_holding(tmp_path, mocker, mock_folio_client):  # noqa
    file_date = "20240108"

    holdings = [
        {
            'id': '3bb4a439-842e-5c8d-b86c-eaad46b6a316',
            'holdingsTypeId': '996f93e2-5b5e-4cf2-9168-33ced1f95eed',
            'permanentLocationId': 'f5c58187-3db6-4bda-b1bf-e5f0717e2149',  # <-- Indicates GSB campus location, so no lines added for print or ebook
        }
    ]

    items = [
        {
            'id': '3251f045-f80c-5c0d-8774-a75af8a6f01c',
        },
    ]

    def mock_folio_get(*args):
        result = folio_result
        result["holdingsRecords"] = holdings
        result["items"] = items
        return result

    mock_folio_client.folio_get = mock_folio_get

    mocker.patch(
        'libsys_airflow.plugins.data_exports.marc.transformer.folio_client',
        return_value=mock_folio_client,
    )

    marc_file = tmp_path / f"{file_date}.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(
            record(
                isbns=["1234567890123", "9876543212345"],
                fields035=["notgls12345", "other1value"],
                fields856=["notgobi", "other"],
                fields956=["notsubscribed", "other"],
            )
        )

    transformer = gobi_transformer.GobiTransformer()
    transformer.generate_list(marc_file)

    gobi_file = pathlib.Path(marc_file.parent / f"stf.{file_date}.txt")

    with gobi_file.open('r+') as fo:
        assert fo.readline() == ''
