import json
import httpx
import pymarc
import pytest

from libsys_airflow.plugins.data_exports import oclc_api


def mock_metadata_session():
    def mock_bib_validate(**kwargs):
        

# def mock_oclc_httpx():
#     def mock_response(request):
#         response = None
#         match request.method:

#             case 'PUT':
#                 if request.url.path.endswith('d63085c0-cab6-4bdd-95e8-d53696919ac1'):
#                     response = httpx.Response(
#                         status_code=422, text='Failed to update SRS'
#                     )
#                 elif request.url.path.startswith('/source-storage/records'):
#                     response = httpx.Response(status_code=200)

#             case 'POST':
#                 if request.url.host.startswith('oauth.oclc.org'):
#                     response = httpx.Response(
#                         status_code=200, json={'access_token': "abcded12345"}
#                     )
#                 if request.url.path.startswith('/worldcat/manage/bibs'):
#                     record = pymarc.Record(data=request.content)
#                     if len(record.get_fields('001')) == 1:
#                         response = httpx.Response(
#                             status_code=500, text='Internal Server Error'
#                         )
#                     else:
#                         record.add_ordered_field(
#                             pymarc.Field(
#                                 tag='035',
#                                 indicators=[' ', ' '],
#                                 subfields=[
#                                     pymarc.Subfield(code='a', value='(OCoLC)123489')
#                                 ],
#                             )
#                         )
#                         response = httpx.Response(
#                             status_code=200, content=record.as_marc21()
#                         )
#                 if request.url.path.startswith("/source-storage/snapshots"):
#                     response = httpx.Response(status_code=200)
#                 elif request.url.path.endswith('/manage/institution/holdings/set'):
#                     record = pymarc.Record(data=request.content)
#                     if len(record.get_fields('001')) == 1:
#                         response = httpx.Response(
#                             status_code=500, text='Internal Server Error'
#                         )
#                     else:
#                         response = httpx.Response(
#                             status_code=200,
#                             json={
#                                 "controlNumber": "439887343",
#                                 "requestedControlNumber": "439887343",
#                                 "institutionCode": "158223",
#                                 "institutionSymbol": "STF",
#                                 "success": True,
#                                 "message": "Holding Updated Successfully",
#                                 "action": "Set Holdings",
#                             },
#                         )

#         return response
# return httpx.Client(transport=httpx.MockTransport(mock_response))

@pytest.fixture
def mock_bookops_package(mocker):
    pass


# @pytest.fixture
def mock_folio_client(mocker):
    mock = mocker
    mock.okapi_headers = {}
    mock.okapi_url = "https://okapi.stanford.edu/"
    return mock


@pytest.fixture
def mock_oclc_api(mocker):
    
    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_api.WorldcatAccessToken",
        return_value="tk_6e302a204c2bfa4d266813cO647d62a77b10"
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_api.folio_client",
        return_value=mock_folio_client(mocker),
    )

    return mocker


def test_oclc_api_class_init(mock_oclc_api):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    assert oclc_api_instance.folio_client.okapi_url == "https://okapi.stanford.edu/"
    assert oclc_api_instance.oclc_token == "tk_6e302a204c2bfa4d266813cO647d62a77b10"


def test_oclc_api_class_no_new_records(mock_oclc_api, caplog):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    oclc_api_instance.new([])

    assert "No new marc records" in caplog.text


def test_oclc_api_class_new_records(tmp_path, mock_oclc_api):

    marc_record = pymarc.Record()
    marc_record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='s', value='08ca5a68-241a-4a5f-89b9-5af5603981ad')
            ],
        )
    )

    no_srs_record = pymarc.Record()
    no_srs_record.add_field(
        pymarc.Field(
            tag='245',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='Much Ado about Something')],
        )
    )
    marc_file = tmp_path / "202403273-STF-new.mrc"

    with marc_file.open('wb') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in [marc_record, no_srs_record]:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    new_result = oclc_api_instance.new(
        [
            str(marc_file.absolute()),
        ]
    )

    assert new_result['success'] == ['08ca5a68-241a-4a5f-89b9-5af5603981ad']
    assert new_result['failures'] == []


def test_oclc_api_class_updated_records(tmp_path, mock_oclc_api):
    marc_record = pymarc.Record()
    marc_record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='s', value='ea5b38dc-8f96-45de-8306-a2dd673716d5')
            ],
        )
    )

    no_srs_record = pymarc.Record()
    no_srs_record.add_field(
        pymarc.Field(
            tag='245',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value='Much Ado about Something')],
        )
    )

    marc_file = tmp_path / "202403273-STF-update.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in [marc_record, no_srs_record]:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    updated_result = oclc_api_instance.update([str(marc_file.absolute())])

    assert updated_result['success'] == ['ea5b38dc-8f96-45de-8306-a2dd673716d5']
    assert updated_result['failures'] == []


def test_oclc_api_failed_authenication(mocker):
    mock_httpx = mocker.MagicMock()
    mock_httpx.Client = httpx.Client(
        transport=httpx.MockTransport(lambda *args: httpx.Response(500))
    )

    mocker.patch.object(oclc_api, "httpx", mock_httpx)

    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_api.folio_client",
        return_value=mock_folio_client,
    )

    with pytest.raises(Exception, match="Unable to Retrieve Access Token"):
       oclc_api.OCLCAPIWrapper(
            client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
            secret="c867b1dd75e6490f99d1cd1c9252ef22"
        )


def test_oclc_api_missing_srs(mock_oclc_api, caplog):

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    record = pymarc.Record()

    oclc_api_instance.__srs_uuid__(record)

    assert "Record Missing SRS uuid" in caplog.text


def test_failed_oclc_new_record(tmp_path, mock_oclc_api):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag='001', data='00a'))
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='s', value='08ca5a68-241a-4a5f-89b9-5af5603981ad')
            ],
        )
    )

    marc_file = tmp_path / "202403273-STF-new.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    new_response = oclc_api_instance.new([str(marc_file.absolute())])

    assert new_response["success"] == []
    assert new_response["failures"] == ['08ca5a68-241a-4a5f-89b9-5af5603981ad']


def test_bad_srs_put_in_new_context(tmp_path, mock_oclc_api):
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='s', value='d63085c0-cab6-4bdd-95e8-d53696919ac1')
            ],
        )
    )

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    marc_file = tmp_path / "202403273-STF-new.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        marc_writer.write(record)

    new_results = oclc_api_instance.new([str(marc_file.absolute())])

    assert new_results['success'] == []
    assert new_results['failures'] == ['d63085c0-cab6-4bdd-95e8-d53696919ac1']


def test_no_update_records(mock_oclc_api, caplog):
    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )
    oclc_api_instance.update([])

    assert "No updated marc records" in caplog.text


def test_bad_holdings_set_call(tmp_path, mock_oclc_api):
    marc_record = pymarc.Record()
    marc_record.add_field(
        pymarc.Field(tag='001', data='4566'),
        pymarc.Field(
            tag='999',
            indicators=['f', 'f'],
            subfields=[
                pymarc.Subfield(code='s', value='ea5b38dc-8f96-45de-8306-a2dd673716d5')
            ],
        ),
    )

    marc_file = tmp_path / "202403273-STF-update.mrc"

    with marc_file.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in [marc_record]:
            marc_writer.write(record)

    oclc_api_instance = oclc_api.OCLCAPIWrapper(
        client_id="EDIoHuhLbdRvOHDjpEBtcEnBHneNtLUDiPRYtAqfTlpOThrxzUwHDUjMGEakoIJSObKpICwsmYZlmpYK", 
        secret="c867b1dd75e6490f99d1cd1c9252ef22"
    )

    update_result = oclc_api_instance.update([str(marc_file.absolute())])

    assert update_result['success'] == []
    assert update_result['failures'] == ['ea5b38dc-8f96-45de-8306-a2dd673716d5']
