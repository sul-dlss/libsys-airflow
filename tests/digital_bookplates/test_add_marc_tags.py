import json
import httpx
import pymarc
import pytest

from libsys_airflow.plugins.shared import utils


def sample_marc_records():
    sample = []
    unique_record = pymarc.Record()
    unique_record.add_field(
        pymarc.Field(
            tag="979",
            indicators=[' ', ' '],
            subfields=[
                pymarc.Subfield(code='f', value='PEART'),
                pymarc.Subfield(code='b', value='druid:yyz2112'),
                pymarc.Subfield(code='c', value='yyz2112mov.pic'),
                pymarc.Subfield(
                    code='d', value='Peart Memorial Fund for Cycle Touring'
                ),
            ],
        )
    )
    sample.append(unique_record)
    duplicate_record = pymarc.Record()
    duplicate_record.add_field(
        pymarc.Field(
            tag="979",
            indicators=[' ', ' '],
            subfields=[
                pymarc.Subfield(code='f', value='ABBOTT'),
                pymarc.Subfield(code='b', value='druid:ws066yy0421'),
                pymarc.Subfield(code='c', value='ws066yy0421_00_0001.jp2'),
                pymarc.Subfield(
                    code='d',
                    value='The The Donald P. Abbott Fund for Marine Invertebrates',
                ),
            ],
        )
    )
    sample.append(duplicate_record)
    return sample


def mock_httpx_client():
    def mock_response(request):
        response = None
        match request.method:

            case 'PUT':
                if request.url.path.startswith('/change-manager/parsedRecords'):
                    response = httpx.Response(status_code=202)

        return response

    return httpx.Client(transport=httpx.MockTransport(mock_response))


def mock_folio_client(mocker):
    sample_marc = sample_marc_records()

    def __srs_response__(path: str):
        output = {}
        instance_uuid = path.split("instanceId=")[-1]

        match instance_uuid:
            case "64a5a15b-d89e-4bdd-bbd6-fcd215b367e4":
                output = {
                    "sourceRecords": [
                        {
                            "recordId": "e5c1d877-5707-4bd7-8576-1e2e69d83e70",
                            "parsedRecord": {
                                "content": json.loads(sample_marc[0].as_json())
                            },
                        }
                    ]
                }

            case "242c6000-8485-5fcd-9b5e-adb60788ca59":
                output = {
                    "sourceRecords": [
                        {
                            "recordId": "e5c1d877-5707-4bd7-8576-1e2e69d83e70",
                            "parsedRecord": {
                                "content": json.loads(sample_marc[1].as_json())
                            },
                        }
                    ]
                }

        return output

    def mock_folio_get(*args, **kwargs):
        output = {}
        if args[0].startswith("/source-storage/source-records"):
            output = __srs_response__(args[0])
        if args[0].startswith("/inventory/instances/"):
            for instance_uuid in [
                "64a5a15b-d89e-4bdd-bbd6-fcd215b367e4",
                "242c6000-8485-5fcd-9b5e-adb60788ca59",
            ]:
                if args[0].endswith(instance_uuid):
                    output = {"_version": "1", "hrid": "a123456"}

        return output

    mock = mocker
    mock.okapi_headers = {}
    mock.okapi_url = "http://okapi:9130"
    mock.folio_get = mock_folio_get
    return mock


@pytest.fixture
def mock_folio_add_marc_tags(mocker):
    mock_httpx = mocker.MagicMock()
    mock_httpx.Client = lambda: mock_httpx_client()

    mocker.patch.object(utils, "httpx", mock_httpx)

    mocker.patch(
        "libsys_airflow.plugins.shared.utils.folio_client",
        return_value=mock_folio_client(mocker),
    )

    return mocker


marc_instances_tags = {
    '979': {
        'ind1': ' ',
        'ind2': ' ',
        'subfields': [
            {'f': 'ABBOTT'},
            {'b': 'druid:ws066yy0421'},
            {'c': 'ws066yy0421_00_0001.jp2'},
            {'d': 'The The Donald P. Abbott Fund for Marine Invertebrates'},
        ],
    }
}


def test_put_folio_records_unique_tag(mock_folio_add_marc_tags, caplog):
    add_marc_tag = utils.FolioAddMarcTags()
    put_record_result = add_marc_tag.put_folio_records(
        marc_instances_tags, "64a5a15b-d89e-4bdd-bbd6-fcd215b367e4"
    )
    assert put_record_result is True
    assert "Skip adding duplicated 979 field" not in caplog.text


def test_put_folio_records_duplicate_tag(mock_folio_add_marc_tags, caplog):
    add_marc_tag = utils.FolioAddMarcTags()
    put_record_result = add_marc_tag.put_folio_records(
        marc_instances_tags, "242c6000-8485-5fcd-9b5e-adb60788ca59"
    )
    assert put_record_result is True
    assert "Skip adding duplicated 979 field" in caplog.text
