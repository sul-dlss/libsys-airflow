import json
import httpx
import pytest

from libsys_airflow.plugins.shared import utils


@pytest.fixture
def marc_json():
    return {
        "leader": "          22        4500",
        "fields": [
            {"001": "a477973"},
            {"007": "cr||||        "},
            {"008": "880901c19819999xxuqr1p       0   a0eng u"},
            {
                "245": {
                    "ind1": "0",
                    "ind2": "0",
                    "subfields": [
                        {"a": "Journal of crustacean biology :"},
                        {
                            "b": "a quarterly of the Crustacean Society for the publication of research on any aspect of the biology of crustacea."
                        },
                    ],
                }
            },
            {
                "260": {
                    "ind1": " ",
                    "ind2": " ",
                    "subfields": [
                        {"a": "[S.l.] :"},
                        {"b": "The Society,"},
                        {"c": "c1981-"},
                    ],
                }
            },
            {
                "999": {
                    "ind1": "f",
                    "ind2": "f",
                    "subfields": [
                        {"i": "06660d4f-982d-54e8-b34c-532c268868e1"},
                        {"s": "e60b77d3-3a76-59e2-88f7-3d1a045af3b1"},
                    ],
                }
            },
        ],
    }


@pytest.fixture
def marc_instance_tags():
    return {
        "979": [
            {
                "ind1": "",
                "ind2": "",
                "subfields": [
                    {"f": "ABBOTT"},
                    {"b": "druid:ws066yy0421"},
                    {"c": "ws066yy0421_00_0001.jp2"},
                    {"d": "The Donald P. Abbott Fund for Marine Invertebrates"},
                ],
            }
        ]
    }


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


def mock_httpx_client():
    def mock_response(request):
        response = None
        # following is not used in test but leaving here for testing more parts of utils.py
        # match request.method:

        #     case 'PUT':
        #         if request.url.path.startswith('/change-manager/parsedRecords'):
        #             response = httpx.Response(status_code=202)

        return response

    return httpx.Client(transport=httpx.MockTransport(mock_response))


def mock_folio_client(mocker):
    # following is not used in test but leaving here for testing more parts of utils.py
    # def __srs_response__(path: str):
    #     output = {}
    #     instance_uuid = path.split("instanceId=")[-1]

    #     match instance_uuid:
    #         case "06660d4f-982d-54e8-b34c-532c268868e1":
    #             output = {
    #                 "sourceRecords": [
    #                     {
    #                         "recordId": "e60b77d3-3a76-59e2-88f7-3d1a045af3b1",
    #                         "parsedRecord": {"content": marc_json},
    #                     }
    #                 ]
    #             }

    #     return output

    def mock_folio_get(*args, **kwargs):
        output = {}
        # following is not used in test but leaving here for testing more parts of utils.py
        # if args[0].startswith("/source-storage/source-records"):
        #     output = __srs_response__(args[0])
        # if args[0].startswith("/inventory/instances/"):
        #     for instance_uuid in [
        #         "64a5a15b-d89e-4bdd-bbd6-fcd215b367e4",
        #         "242c6000-8485-5fcd-9b5e-adb60788ca59",
        #     ]:
        #         if args[0].endswith(instance_uuid):
        #             output = {"_version": "1", "hrid": "a123456"}

        return output

    mock = mocker
    mock.okapi_headers = {}
    mock.okapi_url = "http://okapi:9130"
    mock.folio_get = mock_folio_get
    return mock


def test__marc_json_with_new_tags__(
    mock_folio_add_marc_tags, marc_json, marc_instance_tags
):
    add_marc_tag = utils.FolioAddMarcTags()
    marc_json_with_new_tags = add_marc_tag.__marc_json_with_new_tags__(
        marc_json, marc_instance_tags
    )
    new_record_dict = json.loads(marc_json_with_new_tags)
    for tag in new_record_dict["fields"]:
        for k, v in tag.items():
            if k == "979":
                assert len(v["subfields"]) == 4
