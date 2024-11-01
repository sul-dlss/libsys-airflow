import json
import httpx
import pymarc
import pytest

from unittest.mock import MagicMock

from bookops_worldcat.errors import WorldcatAuthorizationError, WorldcatRequestError

from libsys_airflow.plugins.data_exports import oclc_api


def sample_marc_records():
    sample = []
    marc_record = pymarc.Record()
    marc_record.add_field(
        pymarc.Field(tag="007", data="cr un         "),
        pymarc.Field(
            tag="040",
            indicators=pymarc.Indicators(" ", " "),
            subfields=[
                pymarc.Subfield("a", "CSt"),
                pymarc.Subfield("b", "eng"),
                pymarc.Subfield("e", "rda"),
                pymarc.Subfield("c", "CSt"),
            ],
        ),
        pymarc.Field(
            tag="100",
            indicators=['1', ''],
            subfields=[pymarc.Subfield(code='a', value='Morrison, Toni')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value="958835d2-39cc-4ab3-9c56-53bf7940421b"),
                pymarc.Subfield(code='s', value='08ca5a68-241a-4a5f-89b9-5af5603981ad'),
            ],
        ),
    )
    sample.append(marc_record)
    no_srs_record = pymarc.Record()
    no_srs_record.add_field(
        pymarc.Field(tag="007", data="cr un     a    "),
        pymarc.Field(
            tag="040",
            indicators=pymarc.Indicators(" ", " "),
            subfields=[
                pymarc.Subfield("a", "CSt"),
                pymarc.Subfield("e", "rda"),
                pymarc.Subfield("c", "CSt"),
            ],
        ),
        pymarc.Field(
            tag='245',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='Much Ado about Something')],
        ),
    )
    sample.append(no_srs_record)
    another_record = pymarc.Record()
    another_record.add_field(
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)445667')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='f19fd2fc-586c-45df-9b0c-127af97aef34'),
                pymarc.Subfield(code='s', value='d63085c0-cab6-4bdd-95e8-d53696919ac1'),
            ],
        ),
    )
    sample.append(another_record)
    return sample


def bad_records():
    sample = []
    marc_record = pymarc.Record()
    marc_record.add_field(
        pymarc.Field(tag='001', data='4566'),
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)ocn456907809')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='8c9447fa-0556-47cc-98af-c8d5e0d763fb'),
                pymarc.Subfield(code='s', value='ea5b38dc-8f96-45de-8306-a2dd673716d5'),
            ],
        ),
    )
    sample.append(marc_record)
    bad_srs_record = pymarc.Record()
    bad_srs_record.add_field(
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)7789932')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='00b492cb-704d-41f4-bd12-74cfe643aea9'),
                pymarc.Subfield(code='s', value='d63085c0-cab6-4bdd-95e8-d53696919ac1'),
            ],
        ),
    )
    sample.append(bad_srs_record)
    no_response_record = pymarc.Record()
    no_response_record.add_field(
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)2369001')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='f8fa3682-fef8-4810-b8da-8f51b73785ac'),
                pymarc.Subfield(code='s', value='6325e8fd-101a-4972-8da7-298cd01d1a9d'),
            ],
        ),
    )
    sample.append(no_response_record)
    failed_response_record = pymarc.Record()
    failed_response_record.add_field(
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)17032778')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='ce4b5983-ff44-4b27-ab3c-d63a38095e30')
            ],
        ),
    )
    sample.append(failed_response_record)
    bad_oclc_num = pymarc.Record()

    bad_oclc_num.add_field(
        pymarc.Field(tag='008', data="a7340114"),
        pymarc.Field(
            tag='035',
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value="(OCoLC)39301853")],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='7063655a-6196-416f-94e7-8d540e014805'),
                pymarc.Subfield(code='s', value='08ca5a68-241a-4a5f-89b9-5af5603981ad'),
            ],
        ),
    )
    sample.append(bad_oclc_num)
    worldcat_error_record = pymarc.Record()
    worldcat_error_record.add_field(
        pymarc.Field(tag='008', data='00a'),
        pymarc.Field(
            tag='500',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='Of noteworthy import')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='e15e3707-f012-482f-a13b-34556b6d0946'),
                pymarc.Subfield(code='s', value='08ca5a68-241a-4a5f-89b9-5af5603981ad'),
            ],
        ),
    )
    sample.append(worldcat_error_record)
    return sample


def missing_or_multiple_oclc_records():
    sample = []
    missing_oclc_record = pymarc.Record()
    missing_oclc_record.add_field(
        pymarc.Field(
            tag="245",
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code="a", value="Various Stuff")],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value="958835d2-39cc-4ab3-9c56-53bf7940421b"),
                pymarc.Subfield(code='s', value='08ca5a68-241a-4a5f-89b9-5af5603981ad'),
            ],
        ),
    )
    sample.append(missing_oclc_record)
    multiple_oclc_record = pymarc.Record()
    multiple_oclc_record.add_field(
        pymarc.Field(
            tag='035',
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)2369001')],
        ),
        pymarc.Field(
            tag='035',
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)456789')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='f19fd2fc-586c-45df-9b0c-127af97aef34'),
                pymarc.Subfield(code='s', value='d63085c0-cab6-4bdd-95e8-d53696919ac1'),
            ],
        ),
    )
    sample.append(multiple_oclc_record)
    return sample


def mock_metadata_session(authorization=None, timeout=None):
    mock_response = MagicMock()

    def mock__enter__(*args):
        return args[0]

    def mock__exit__(*args):
        pass

    def mock_bib_create(**kwargs):

        record = pymarc.Record(data=kwargs['record'])
        if "008" in record and record["008"].value() == "00a":
            raise WorldcatRequestError(b"400 Client Error")

        if "024" in record and record["024"]["a"] == "gtfo-78901":
            record.add_field(
                pymarc.Field(
                    tag='035',
                    indicators=['', ''],
                    subfields=[pymarc.Subfield(code='b', value='39710')],
                )
            )
        else:
            record.add_field(
                pymarc.Field(
                    tag='035',
                    indicators=['', ''],
                    subfields=[pymarc.Subfield(code='a', value='(OCoLC)345678')],
                )
            )
        mock_response.content = record.as_marc21()
        return mock_response

    def mock_bib_match(**kwargs):
        raw_record = kwargs["record"]

        record = pymarc.Record(data=raw_record)

        payload = {"numberOfRecords": 1, "briefRecords": []}

        match record['008'].value():

            case "a4589":
                payload['briefRecords'].append({'oclcNumber': "8891340"})

            case "a9007876":
                payload['numberOfRecords'] = 0

            case "a60987":
                payload['briefRecords'].append({'oclcNumber': '456907809'})

            case "a20017876":
                payload['briefRecords'].append({'oclcNumber': '445667'})

            case "a7340114":
                payload['briefRecords'].append({'oclcNumber': '39301853'})

        mock_response.json = lambda: payload
        return mock_response

    def mock_holdings_set(**kwargs):
        payload = {
            "controlNumber": "439887343",
            "requestedControlNumber": "439887343",
            "institutionCode": "158223",
            "institutionSymbol": "STF",
            "success": True,
            "message": "Holding Updated Successfully",
            "action": "Set Holdings",
        }
        if kwargs['oclcNumber'] == "456907809":
            raise WorldcatRequestError(b"400 Client Error")
        if kwargs['oclcNumber'] in ['2369001', '889765']:
            return
        if kwargs['oclcNumber'] == "39301853":
            payload["success"] = False
            payload["controlNumber"] = kwargs['oclcNumber']
            payload["requestedControlNumber"] = kwargs['oclcNumber']
            payload["message"] = "Holding Updated Failed"
        mock_response.json = lambda: payload
        return mock_response

    def mock_holdings_unset(**kwargs):
        if kwargs["oclcNumber"] == "456907809":
            raise WorldcatRequestError(b"400 Client Error")
        if kwargs["oclcNumber"] == "2369001":
            return
        mock_response.json = lambda: {
            'controlNumber': '1125353',
            'requestedControlNumber': '1125353',
            'institutionCode': '158223',
            'institutionSymbol': 'STF',
            'firstTimeUse': False,
            'success': True,
            'message': 'Unset Holdings Succeeded.',
            'action': 'Unset Holdings',
        }
        return mock_response

    mock_session = MagicMock()
    # Supports mocking context manager
    mock_session.__enter__ = mock__enter__
    mock_session.__exit__ = mock__exit__
    mock_session._url_manage_bibs_create = (
        lambda: 'https://metadata.api.oclc.org/worldcat'
    )
    mock_session.authorization.token_str = "ab3565ackelas"
    mock_session.bib_create = mock_bib_create
    mock_session.bib_match = mock_bib_match
    mock_session.holdings_set = mock_holdings_set
    mock_session.holdings_unset = mock_holdings_unset

    return mock_session


def mock_worldcat_access_token(**kwargs):
    if kwargs.get('key', '').startswith('n0taVal1dC1i3nt'):
        raise WorldcatAuthorizationError(
            b'{"code":401,"message":"No valid authentication credentials found in request"}'
        )
    return "tk_6e302a204c2bfa4d266813cO647d62a77b10"


def mock_httpx_client():

    def mock_response(request):
        response = None
        match request.method:

            case 'PUT':
                if request.url.path.endswith('4c2d955a-b024-448f-9f7a-6fc1f51416d8'):
                    response = httpx.Response(
                        status_code=422, text='Failed to update SRS'
                    )
                elif request.url.path.startswith('/change-manager/parsedRecords'):
                    response = httpx.Response(status_code=202)

            case 'POST':
                record = pymarc.Record(data=request.content)
                if "008" in record and record["008"].value() == "00a":
                    response = httpx.Response(
                        status_code=400, content=b'400 Client Error'
                    )

                elif "024" in record and record["024"]["a"] == "gtfo-78901":
                    record.add_field(
                        pymarc.Field(
                            tag='035',
                            indicators=['', ''],
                            subfields=[pymarc.Subfield(code='b', value='39710')],
                        )
                    )
                    response = httpx.Response(
                        status_code=200, content=record.as_marc21()
                    )
                else:
                    record.add_field(
                        pymarc.Field(
                            tag='035',
                            indicators=['', ''],
                            subfields=[
                                pymarc.Subfield(code='a', value='(OCoLC)345678')
                            ],
                        )
                    )
                    response = httpx.Response(
                        status_code=200, content=record.as_marc21()
                    )

        return response

    return httpx.Client(transport=httpx.MockTransport(mock_response))


def mock_folio_client(mocker):
    sample_marc = sample_marc_records()

    def __srs_response__(path: str):
        output = {}
        instance_uuid = path.split("instanceId=")[-1]

        match instance_uuid:
            case "958835d2-39cc-4ab3-9c56-53bf7940421b":
                output = {
                    "sourceRecords": [
                        {
                            "recordId": "e941404e-2dab-4a34-8aa5-3dcaef62736b",
                            "parsedRecord": {
                                "content": json.loads(sample_marc[0].as_json())
                            },
                        }
                    ]
                }

            case (
                "d63085c0-cab6-4bdd-95e8-d53696919ac1"
                | '7063655a-6196-416f-94e7-8d540e014805'
            ):
                output = {
                    "sourceRecords": [
                        {
                            "recordId": instance_uuid,
                            "parsedRecord": {
                                "content": json.loads(sample_marc[2].as_json())
                            },
                        }
                    ]
                }

            case "38a7bb66-cd11-4af6-a339-c13f5855b36f":
                output = {
                    "sourceRecords": [
                        {
                            "recordId": "4c2d955a-b024-448f-9f7a-6fc1f51416d8",
                            "parsedRecord": {
                                "content": json.loads(sample_marc[1].as_json())
                            },
                        }
                    ]
                }

            case "6aabb9cd-64cc-4673-b63b-d35fa015b91c":
                output = {"sourceRecords": []}
        return output

    def mock_folio_get(*args, **kwargs):
        output = {}
        if args[0].startswith("/source-storage/source-records"):
            output = __srs_response__(args[0])
        if args[0].startswith("/inventory/instances/"):
            for instance_uuid in [
                "f19fd2fc-586c-45df-9b0c-127af97aef34",
                "958835d2-39cc-4ab3-9c56-53bf7940421b",
                "00b492cb-704d-41f4-bd12-74cfe643aea9",
                "38a7bb66-cd11-4af6-a339-c13f5855b36f",
                "a3a6f1c4-152c-4f8f-9763-07a49cd6fa5a",
                "2023473e-802a-4bd2-9ca1-5d2e360a0fbd",
                "ce4b5983-ff44-4b27-ab3c-d63a38095e30",
                "7063655a-6196-416f-94e7-8d540e014805",
                "f8fa3682-fef8-4810-b8da-8f51b73785ac",
                "8c9447fa-0556-47cc-98af-c8d5e0d763fb",
            ]:
                if args[0].endswith(instance_uuid):
                    output = {"_version": "2", "hrid": "a345691"}

        return output

    mock = mocker
    mock.okapi_headers = {}
    mock.okapi_url = "https://okapi.stanford.edu"
    mock.folio_get = mock_folio_get
    return mock


@pytest.fixture
def mock_oclc_api(mocker):
    mock_httpx = mocker.MagicMock()
    mock_httpx.Client = lambda: mock_httpx_client()

    mocker.patch.object(oclc_api, "httpx", mock_httpx)

    mocker.patch.object(oclc_api, "WorldcatAccessToken", mock_worldcat_access_token)

    mocker.patch.object(oclc_api, "MetadataSession", mock_metadata_session)

    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_api.folio_client",
        return_value=mock_folio_client(mocker),
    )

    return mocker


def test_oclc_api_class_init(mock_oclc_api):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    assert oclc_api_instance.folio_client.okapi_url == "https://okapi.stanford.edu"
    assert oclc_api_instance.oclc_token == "tk_6e302a204c2bfa4d266813cO647d62a77b10"


def test_oclc_api_class_no_new_records(mock_oclc_api, caplog):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    oclc_api_instance.new([])

    assert "No new marc records" in caplog.text


def test_oclc_api_class_new_records(tmp_path, mock_oclc_api):
    marc_record, no_srs_record, _ = sample_marc_records()
    marc_file = tmp_path / "202403273-STF-new.mrc"

    with marc_file.open('wb') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in [marc_record, no_srs_record]:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    new_result = oclc_api_instance.new(
        [
            str(marc_file.absolute()),
        ]
    )

    assert new_result['success'] == ['958835d2-39cc-4ab3-9c56-53bf7940421b']
    assert new_result['failures'] == []


def test_oclc_api_class_updated_records(tmp_path, mock_oclc_api):
    marc_record, no_srs_record, _ = sample_marc_records()
    marc_record.add_ordered_field(
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC-M)on455677')],
        )
    )

    marc_file = tmp_path / "202403273-STF-update.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in [marc_record, no_srs_record]:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    updated_result = oclc_api_instance.update([str(marc_file.absolute())])

    assert updated_result['success'] == ['958835d2-39cc-4ab3-9c56-53bf7940421b']
    assert updated_result['failures'] == []
    assert updated_result['archive'] == [str(marc_file.absolute())]


def test_oclc_api_failed_authentication(mock_oclc_api):

    with pytest.raises(Exception, match="Unable to Retrieve Worldcat Access Token"):
        oclc_api.OCLCAPIWrapper(
            client_id="n0taVal1dC1i3nt", secret="c867b1dd75e6490f99d1cd1c9252ef22"
        )


def test_oclc_api_missing_instance_uuid(mock_oclc_api, caplog):

    record = pymarc.Record()

    oclc_api.get_instance_uuid(record)

    assert "No instance UUID found in MARC record" in caplog.text


def test_failed_oclc_new_record(tmp_path, mock_oclc_api):
    error_records = bad_records()

    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)889765')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='958835d2-39cc-4ab3-9c56-53bf7940421b'),
                pymarc.Subfield(code='s', value='8cd20c2e-a2e6-47c8-8f33-dca928c6b4c0'),
            ],
        ),
    )

    marc_file = tmp_path / "202403273-STF-new.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in [error_records[1], error_records[4], error_records[5], record]:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    new_response = oclc_api_instance.new([str(marc_file.absolute())])

    sorted_errors = sorted(new_response["failures"], key=lambda x: x['uuid'])

    assert new_response["success"] == []

    assert sorted_errors[0] == {
        'uuid': '00b492cb-704d-41f4-bd12-74cfe643aea9',
        'reason': 'FOLIO failed to Add OCLC number',
        'context': '7789932',
    }

    assert sorted_errors[1] == {
        'uuid': '7063655a-6196-416f-94e7-8d540e014805',
        'reason': 'Failed to update holdings for new record',
        'context': {
            'controlNumber': '39301853',
            'requestedControlNumber': '39301853',
            'institutionCode': '158223',
            'institutionSymbol': 'STF',
            'success': False,
            'message': 'Holding Updated Failed',
            'action': 'Set Holdings',
        },
    }

    assert sorted_errors[3] == {
        'uuid': '958835d2-39cc-4ab3-9c56-53bf7940421b',
        'reason': 'No response from OCLC',
        'context': None,
    }

    assert sorted_errors[4]['uuid'] == "958835d2-39cc-4ab3-9c56-53bf7940421b"

    assert sorted_errors[5] == {
        "uuid": 'e15e3707-f012-482f-a13b-34556b6d0946',
        "reason": "Failed to add new MARC record",
        "context": "400 Client Error",
    }


def test_new_no_control_number(mock_oclc_api, tmp_path):
    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    instance_uuid = "a3b25c31-04c0-44d3-b894-e6e41f128f32"
    marc_file = tmp_path / "2024061913-CASUM.mrc"

    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='024',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value="gtfo-78901")],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[pymarc.Subfield(code='i', value=instance_uuid)],
        ),
    )

    with marc_file.open('wb+') as fo:
        writer = pymarc.MARCWriter(fo)
        writer.write(record)

    new_response = oclc_api_instance.new([str(marc_file.absolute())])

    assert new_response["success"] == []
    assert new_response["failures"] == [
        {
            "uuid": instance_uuid,
            "reason": 'Failed to extract OCLC number',
            "context": None,
        }
    ]


def test_bad_srs_put_in_new_context(tmp_path, mock_oclc_api):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    marc_file = tmp_path / "202403273-STF-new.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(sample_marc_records()[2])

    new_results = oclc_api_instance.new([str(marc_file.absolute())])

    assert new_results['success'] == []
    assert new_results['failures'] == [
        {
            "uuid": 'f19fd2fc-586c-45df-9b0c-127af97aef34',
            "reason": "FOLIO failed to Add OCLC number",
            "context": "445667",
        }
    ]
    assert new_results['archive'] == []


def test_no_update_records(mock_oclc_api, caplog):
    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )
    oclc_api_instance.update([])

    assert "No updated marc records" in caplog.text


def test_already_exists_control_number(tmp_path, mock_oclc_api):
    marc_record = pymarc.Record()
    marc_record.add_field(
        pymarc.Field(tag='001', data='4566'),
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)445667')],
        ),
    )

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    modified_marc_record = oclc_api_instance.__update_oclc_number__(
        '445667', marc_record
    )

    assert modified_marc_record.get_fields('035')[0].value() == "(OCoLC-M)445667"


def test_missing_srs_record_id(mock_oclc_api, caplog):
    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    srs_record_id = oclc_api_instance.__get_srs_record_id__(
        "6aabb9cd-64cc-4673-b63b-d35fa015b91c"
    )

    assert srs_record_id is None
    assert (
        "No Active SRS record found for 6aabb9cd-64cc-4673-b63b-d35fa015b91c"
        in caplog.text
    )


def test_oclc_update_errors(mock_oclc_api, caplog, tmp_path):

    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='035',
            indicators=['', ''],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)889765')],
        ),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='a3a6f1c4-152c-4f8f-9763-07a49cd6fa5a'),
                pymarc.Subfield(code='s', value='8cd20c2e-a2e6-47c8-8f33-dca928c6b4c0'),
            ],
        ),
    )

    error_records = bad_records()

    marc_file = tmp_path / "2024042413-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)
        for record in error_records[3:5]:
            marc_writer.write(record)
        for record in missing_or_multiple_oclc_records():
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    update_results = oclc_api_instance.update([str(marc_file)])

    sorted_errors = sorted(update_results["failures"], key=lambda x: x['uuid'])

    assert sorted_errors[0]['context'] == {
        'controlNumber': '39301853',
        'requestedControlNumber': '39301853',
        'institutionCode': '158223',
        'institutionSymbol': 'STF',
        'success': False,
        'message': 'Holding Updated Failed',
        'action': 'Set Holdings',
    }

    assert sorted_errors[1] == {
        "uuid": "958835d2-39cc-4ab3-9c56-53bf7940421b",
        "reason": "Missing OCLC number",
        "context": None,
    }

    assert sorted_errors[2] == {
        'uuid': 'a3a6f1c4-152c-4f8f-9763-07a49cd6fa5a',
        'reason': 'No response from OCLC',
        'context': None,
    }

    assert sorted_errors[3] == {
        'uuid': 'ce4b5983-ff44-4b27-ab3c-d63a38095e30',
        'reason': 'FOLIO failed to Add OCLC number',
        'context': '439887343',
    }

    assert sorted_errors[4] == {
        "uuid": "f19fd2fc-586c-45df-9b0c-127af97aef34",
        "reason": "Multiple OCLC ids",
        "context": ['2369001', '456789'],
    }


def test_failed_folio_put(mock_oclc_api, caplog):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    put_result = oclc_api_instance.__put_folio_record__(
        "38a7bb66-cd11-4af6-a339-c13f5855b36f", pymarc.Record()
    )

    assert put_result is False


def test_delete_missing_or_multiple_oclc_numbers(mock_oclc_api, tmp_path):
    marc_file = tmp_path / "2024060612-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in missing_or_multiple_oclc_records():
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    results = oclc_api_instance.delete([str(marc_file)])

    sorted_failures = sorted(results["failures"], key=lambda x: x['uuid'])

    assert sorted_failures[0] == {
        "uuid": "958835d2-39cc-4ab3-9c56-53bf7940421b",
        "reason": "Missing OCLC number",
        "context": None,
    }

    assert sorted_failures[1] == {
        "uuid": "f19fd2fc-586c-45df-9b0c-127af97aef34",
        "reason": 'Multiple OCLC ids',
        "context": ['2369001', '456789'],
    }


def test_delete_result_success_and_errors(mock_oclc_api, tmp_path, caplog):
    error_records = bad_records()
    good_record = sample_marc_records()[2]

    marc_file = tmp_path / "2024070114-STF.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in [error_records[0], error_records[2], good_record]:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    results = oclc_api_instance.delete([str(marc_file.absolute())])

    failures = sorted(results['failures'], key=lambda x: x['uuid'])

    assert failures[0]['context'] == "b'400 Client Error'"
    assert failures[1]['reason'] == 'Failed holdings_unset'
    assert results['success'] == ['f19fd2fc-586c-45df-9b0c-127af97aef34']


def test_match_oclc_number(mock_oclc_api, tmp_path, caplog):
    existing_record = pymarc.Record()

    existing_record.add_field(
        pymarc.Field(tag='008', data="a4589"),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='958835d2-39cc-4ab3-9c56-53bf7940421b')
            ],
        ),
    )

    new_record = pymarc.Record()

    new_record.add_field(
        pymarc.Field(tag='008', data="a9007876"),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='i', value='2023473e-802a-4bd2-9ca1-5d2e360a0fbd')
            ],
        ),
    )

    bad_oclc_num = bad_records()[-2]

    bad_srs_record = sample_marc_records()[2]

    bad_srs_record.add_ordered_field(
        pymarc.Field(tag='008', data="a20017876"),
    )

    marc_file = tmp_path / "2024070114-STF.mrc"

    with marc_file.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for row in [existing_record, new_record, bad_oclc_num, bad_srs_record]:
            marc_writer.write(row)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    result = oclc_api_instance.match([str(marc_file)])

    assert result["success"] == ['958835d2-39cc-4ab3-9c56-53bf7940421b']

    sorted_failures = sorted(result["failures"], key=lambda x: x['uuid'])

    assert sorted_failures[0] == {
        "uuid": '2023473e-802a-4bd2-9ca1-5d2e360a0fbd',
        "reason": 'Match failed',
        "context": {'numberOfRecords': 0, 'briefRecords': []},
    }

    assert sorted_failures[1] == {
        "uuid": '7063655a-6196-416f-94e7-8d540e014805',
        "reason": 'Failed to update holdings after match',
        "context": {
            'controlNumber': '39301853',
            'requestedControlNumber': '39301853',
            'institutionCode': '158223',
            'institutionSymbol': 'STF',
            'success': False,
            'message': 'Holding Updated Failed',
            'action': 'Set Holdings',
        },
    }

    assert sorted_failures[2] == {
        "uuid": 'f19fd2fc-586c-45df-9b0c-127af97aef34',
        "reason": "FOLIO failed to Add OCLC number",
        "context": '445667',
    }

    assert "Sets new holdings for 958835d2-39cc-4ab3-9c56-53bf7940421b" in caplog.text


def test_oclc_records_operation_no_records(mock_oclc_api, caplog):
    connections = {"STF": {"username": "sul-admin", "password": "123245"}}
    test_records_dict = {"STF": []}

    result = oclc_api.oclc_records_operation(
        oclc_function="delete",
        connections=connections,
        records=test_records_dict,
    )

    assert result['success']['STF'] == []
    assert "No delete records for STF" in caplog.text


def test_oclc_marc_modifications():
    sample_records = sample_marc_records()
    mod_record = oclc_api.__oclc_marc_modifications__(sample_records[0])

    assert mod_record["007"].value() == "cr un|||||||||"
    assert mod_record["040"]["a"] == "STF"
    assert mod_record["040"]["b"] == "eng"

    second_mod = oclc_api.__oclc_marc_modifications__(sample_records[1])

    assert second_mod["007"].value() == "cr un|||||a||||"
    assert second_mod["040"]["a"] == "STF"


def test_oclc_records_operation(mocker, mock_oclc_api, tmp_path):
    connections = {"STF": {"username": "sul-admin", "password": "123245"}}

    marc_file = tmp_path / "2024070113-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in missing_or_multiple_oclc_records():
            marc_writer.write(record)

    test_records_dict = {"STF": [str(marc_file.absolute)]}

    result = oclc_api.oclc_records_operation(
        oclc_function="delete", connections=connections, records=test_records_dict
    )

    assert result['success'] == {'STF': []}
    assert result['failures'] == {'STF': []}


def test_worldcat_error(mocker, mock_oclc_api, tmp_path):

    marc_file = tmp_path / "20240821-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(bad_records()[0])

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    result = oclc_api_instance.update([str(marc_file.absolute())])

    assert result["failures"][0]["reason"] == "WorldcatRequest Error"
