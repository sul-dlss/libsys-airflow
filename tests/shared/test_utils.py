import json
import httpx
import pymarc
import pytest

from jsonpath_ng.ext import parse
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
def marc_979():
    return [
        {
            "979": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                    {"f": "STEINMETZ"},
                    {"b": "druid:nc092rd1979"},
                    {"c": "nc092rd1979_00_0001.jp2"},
                    {"d": "Verna Pace Steinmetz Endowed Book Fund in History"},
                ],
            },
        },
        {
            "979": {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                    {"f": "WHITEHEAD"},
                    {"b": "druid:ph944pq1002"},
                    {"c": "ph944pq1002_00_0001.jp2"},
                    {"d": "Barry Whitehead Memorial Book Fund"},
                ],
            },
        },
    ]


@pytest.fixture
def marc_instance_tags():
    return {
        "979": [
            {
                "ind1": " ",
                "ind2": " ",
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
def marc_instance_two_tags():
    return {
        "979": [
            {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                    {"f": "STEINMETZ"},
                    {"b": "druid:nc092rd1979"},
                    {"c": "nc092rd1979_00_0001.jp2"},
                    {"d": "Verna Pace Steinmetz Endowed Book Fund in History"},
                ],
            },
            {
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                    {"f": "WHITEHEAD"},
                    {"b": "druid:ph944pq1002"},
                    {"c": "ph944pq1002_00_0001.jp2"},
                    {"d": "Barry Whitehead Memorial Book Fund"},
                ],
            },
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
        return response

    return httpx.Client(transport=httpx.MockTransport(mock_response))


def mock_folio_client(mocker):

    def mock_folio_get(*args, **kwargs):
        output = {}
        return output

    mock = mocker
    mock.okapi_headers = {}
    mock.okapi_url = "http://okapi:9130"
    mock.folio_get = mock_folio_get
    return mock


def test__marc_json_with_new_tags__(
    mock_folio_add_marc_tags, marc_json, marc_instance_tags, caplog
):
    add_marc_tag = utils.FolioAddMarcTags()
    marc_json_with_new_tags = add_marc_tag.__marc_json_with_new_tags__(
        marc_json, marc_instance_tags
    )
    new_record_dict = json.loads(marc_json_with_new_tags)
    tag_979_exp = parse("$.fields[?(@['979'])]")
    tag_979 = tag_979_exp.find(new_record_dict)[0].value
    assert len(tag_979["979"]["subfields"]) == 4
    assert "New field 979 is unique tag." in caplog.text


def test__marc_json_with_two_new_tags__(
    mock_folio_add_marc_tags, marc_json, marc_instance_two_tags, caplog
):
    add_marc_tag = utils.FolioAddMarcTags()
    marc_json_with_new_tags = add_marc_tag.__marc_json_with_new_tags__(
        marc_json, marc_instance_two_tags
    )
    new_record_dict = json.loads(marc_json_with_new_tags)
    tag_979_exp = parse("$.fields[?(@['979'])]")
    new_979_tags = tag_979_exp.find(new_record_dict)
    assert len(new_979_tags) == 2
    assert (
        "Record does not have existing 979's. New fields will be added." in caplog.text
    )


def test__marc_json_existing_tags__(
    mock_folio_add_marc_tags, marc_json, marc_979, marc_instance_two_tags, caplog
):
    add_marc_tag = utils.FolioAddMarcTags()
    marc_json["fields"].extend(marc_979)
    marc_json_with_new_tags = add_marc_tag.__marc_json_with_new_tags__(
        marc_json, marc_instance_two_tags
    )
    new_record_dict = json.loads(marc_json_with_new_tags)
    tag_979_exp = parse("$.fields[?(@['979'])]")
    new_979_tags = tag_979_exp.find(new_record_dict)
    assert len(new_979_tags) == 2
    assert (
        "Record has existing 979's. New fields will be evaluated for uniqueness."
        in caplog.text
    )
    assert (
        r"Skip adding duplicated =979  \\$fSTEINMETZ$bdruid:nc092rd1979$cnc092rd1979_00_0001.jp2$dVerna Pace Steinmetz Endowed Book Fund in History field"
        in caplog.text
    )
    assert (
        r"Skip adding duplicated =979  \\$fWHITEHEAD$bdruid:ph944pq1002$cph944pq1002_00_0001.jp2$dBarry Whitehead Memorial Book Fund field"
        in caplog.text
    )
    assert "New field 979 is not unique" in caplog.text


def test__tag_is_unique__(mock_folio_add_marc_tags, marc_json, caplog):
    add_marc_tag = utils.FolioAddMarcTags()
    reader = pymarc.reader.JSONReader(json.dumps(marc_json))
    record = [record for record in reader][0]
    existing_tags = [str(field) for field in record.get_fields("979")]

    new_field = pymarc.Field(
        tag="979",
        indicators=[" ", " "],
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
    assert add_marc_tag.__tag_is_unique__(existing_tags, new_field) is True
    assert "tag is unique" in caplog.text


def test_folio_name(mocker):
    mocker.patch(
        "libsys_airflow.plugins.shared.utils.Variable.get",
        return_value="https://okapi-stage.edu/",
    )
    assert utils.folio_name() == "Stage"
    mocker.patch(
        "libsys_airflow.plugins.shared.utils.Variable.get",
        return_value="https://okapi-test.edu/",
    )
    assert utils.folio_name() == "Test"
    mocker.patch(
        "libsys_airflow.plugins.shared.utils.Variable.get",
        return_value="https://okapi-dev.edu/",
    )
    assert utils.folio_name() == "Dev"
    mocker.patch(
        "libsys_airflow.plugins.shared.utils.Variable.get",
        return_value="https://okapi.edu/",
    )
    assert utils.folio_name() is None
