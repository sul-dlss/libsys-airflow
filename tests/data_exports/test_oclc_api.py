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
        pymarc.Field(
            tag='245',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='Much Ado about Something')],
        )
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


def mock_metadata_session(authorization=None):
    mock_response = MagicMock()

    def mock__enter__(*args):
        return args[0]

    def mock__exit__(*args):
        pass

    def mock_bib_create(**kwargs):
        record = pymarc.Record(data=kwargs['record'])
        if "008" in record and record["008"].value() == "00a":
            raise WorldcatRequestError(b"400 Client Error")
        record.add_field(
            pymarc.Field(
                tag='035',
                indicators=['', ''],
                subfields=[pymarc.Subfield(code='a', value='(OCoLC)345678')],
            )
        )
        mock_response.text = record.as_marc21()
        return mock_response

    def mock_holdings_set(**kwargs):
        if kwargs['oclcNumber'] == "456907809":
            raise WorldcatRequestError(b"400 Client Error")
        if kwargs['oclcNumber'] == '2369001':
            return
        mock_response.json = lambda: {
            "controlNumber": "439887343",
            "requestedControlNumber": "439887343",
            "institutionCode": "158223",
            "institutionSymbol": "STF",
            "success": True,
            "message": "Holding Updated Successfully",
            "action": "Set Holdings",
        }
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
    mock_session.bib_create = mock_bib_create
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
                if request.url.path.endswith('d63085c0-cab6-4bdd-95e8-d53696919ac1'):
                    response = httpx.Response(
                        status_code=422, text='Failed to update SRS'
                    )
                elif request.url.path.startswith('/change-manager/parsedRecords'):
                    response = httpx.Response(status_code=202)

        return response

    return httpx.Client(transport=httpx.MockTransport(mock_response))


def mock_folio_client(mocker):
    sample_marc = sample_marc_records()

    def mock_folio_get(*args, **kwargs):
        output = {}
        if args[0].endswith("08ca5a68-241a-4a5f-89b9-5af5603981ad"):
            output = {"parsedRecord": {"content": json.loads(sample_marc[0].as_json())}}
        if args[0].endswith("d63085c0-cab6-4bdd-95e8-d53696919ac1"):
            output = {"parsedRecord": {"content": json.loads(sample_marc[2].as_json())}}
        if args[0].endswith("6aabb9cd-64cc-4673-b63b-d35fa015b91c"):
            output = {}
        for instance_uuid in [
            "f19fd2fc-586c-45df-9b0c-127af97aef34",
            "958835d2-39cc-4ab3-9c56-53bf7940421b",
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


def test_oclc_api_missing_srs(mock_oclc_api, caplog):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    record = pymarc.Record()

    oclc_api_instance.__record_uuids__(record)

    assert "Record Missing SRS uuid" in caplog.text


def test_failed_oclc_new_record(tmp_path, mock_oclc_api):
    record = pymarc.Record()
    record.add_field(
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

    marc_file = tmp_path / "202403273-STF-new.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    new_response = oclc_api_instance.new([str(marc_file.absolute())])

    assert new_response["success"] == []
    assert new_response["failures"] == ['e15e3707-f012-482f-a13b-34556b6d0946']


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
    assert new_results['failures'] == ['f19fd2fc-586c-45df-9b0c-127af97aef34']
    assert new_results['archive'] == []


def test_no_update_records(mock_oclc_api, caplog):
    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )
    oclc_api_instance.update([])

    assert "No updated marc records" in caplog.text


def test_bad_holdings_set_call(tmp_path, mock_oclc_api, caplog):
    error_records = bad_records()

    marc_file = tmp_path / "202403273-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in error_records:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    update_result = oclc_api_instance.update([str(marc_file.absolute())])

    assert update_result['success'] == []
    assert sorted(update_result['failures']) == [
        '00b492cb-704d-41f4-bd12-74cfe643aea9',
        '8c9447fa-0556-47cc-98af-c8d5e0d763fb',
        'f8fa3682-fef8-4810-b8da-8f51b73785ac',
    ]

    assert "Failed to update FOLIO for Instance" in caplog.text


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
        '445667', 'd63085c0-cab6-4bdd-95e8-d53696919ac1'
    )

    assert modified_marc_record.get_fields('035')[0].value() == "(OCoLC-M)445667"


def test_missing_marc_json(mock_oclc_api, caplog):
    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    update_035_result = oclc_api_instance.__update_035__(
        b"", "6aabb9cd-64cc-4673-b63b-d35fa015b91c"
    )

    assert update_035_result is False
    assert "Failed converting 6aabb9cd-64cc-4673-b63b-d35fa015b91c" in caplog.text

    update_oclc_number_record = oclc_api_instance.__update_oclc_number__(
        "22345", "6aabb9cd-64cc-4673-b63b-d35fa015b91c"
    )
    assert update_oclc_number_record is None


def test_missing_or_multiple_oclc_numbers(mock_oclc_api, caplog, tmp_path):

    marc_file = tmp_path / "2024042413-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in missing_or_multiple_oclc_records():
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    update_results = oclc_api_instance.update([str(marc_file)])

    assert sorted(update_results["failures"]) == [
        "958835d2-39cc-4ab3-9c56-53bf7940421b",
        "f19fd2fc-586c-45df-9b0c-127af97aef34",
    ]


def test_no_delete_records(mock_oclc_api, caplog):
    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )
    oclc_api_instance.delete([])

    assert "No marc records for deletes" in caplog.text


def test_delete_records_success(mock_oclc_api, tmp_path):
    marc_record = pymarc.Record()
    marc_record.add_field(
        pymarc.Field(
            tag='035',
            indicators=[" ", " "],
            subfields=[pymarc.Subfield(code='a', value='(OCoLC)1125353')],
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

    marc_file = tmp_path / "202406061104-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(marc_record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    result = oclc_api_instance.delete([str(marc_file)])

    assert result["success"] == ['f19fd2fc-586c-45df-9b0c-127af97aef34']
    assert result['archive'] == [str(marc_file)]


def test_delete_errors(mock_oclc_api, caplog, tmp_path):

    marc_file = tmp_path / "2024060611-STF.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in bad_records():
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK",
        secret="c867b1dd75e6490f99d1cd1c9252ef22",
    )

    result = oclc_api_instance.delete([str(marc_file)])

    assert result['success'] == ['00b492cb-704d-41f4-bd12-74cfe643aea9']
    assert sorted(result['failures']) == [
        '8c9447fa-0556-47cc-98af-c8d5e0d763fb',
        'f8fa3682-fef8-4810-b8da-8f51b73785ac',
    ]
    assert result['archive'] == []


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

    assert sorted(results["failures"]) == [
        "958835d2-39cc-4ab3-9c56-53bf7940421b",
        "f19fd2fc-586c-45df-9b0c-127af97aef34",
    ]
