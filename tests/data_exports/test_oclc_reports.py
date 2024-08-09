import copy
import datetime
import pathlib

import pytest

from bs4 import BeautifulSoup

from libsys_airflow.plugins.data_exports.oclc_reports import (
    filter_failures_task,
    holdings_set_errors_task,
    holdings_unset_errors_task,
    multiple_oclc_numbers_task,
)


@pytest.fixture
def mock_dag_run(mocker):
    mock_dag = mocker.MagicMock()
    mock_dag.url = "https://folio-airflow.edu/scheduled__2024-07-29T19:00:00:00:00"
    return mock_dag


@pytest.fixture
def mock_task_instance(mocker):

    def mock_xcom_pull(**kwargs):
        output = []
        match kwargs['task_ids']:

            case 'divide_delete_records_by_library':
                output = [
                    ('f82cb6d4-2d8a-41af-aa32-72b7ad881e3c', 'CASUM', ['4', '67']),
                    ('16f3b2ba-caf3-467e-bcb1-9463356bd5fb', 'STF', ['89', '91']),
                ]

            case 'divide_new_records_by_library':
                output = [
                    (
                        'eb20e968-3f24-4987-9635-55fb7b1f906c',
                        'STF',
                        ['276045320', '145453718'],
                    )
                ]

        return output

    mock_ti = mocker.MagicMock()
    mock_ti.xcom_pull = mock_xcom_pull
    return mock_ti


ERRORS = [
    {
        "uuid": 'f8fa3682-fef8-4810-b8da-8f51b73785ac',
        "reason": "Failed holdings_unset",
        "context": None,
    },
    {
        "uuid": '8c9447fa-0556-47cc-98af-c8d5e0d763fb',
        "reason": "WorldcatRequest Error",
        "context": "b'400 Client Error'",
    },
    {
        "uuid": '2023473e-802a-4bd2-9ca1-5d2e360a0fbd',
        "reason": 'Match failed',
        "context": {'numberOfRecords': 0, 'briefRecords': []},
    },
    {
        'uuid': '16da5eed-69e7-46da-9d6d-6077543e3e01',
        "reason": "WorldcatRequest Error",
        "context": {
            'errorCount': 21,
            'errors': [
                'Invalid code in Encoding Level (Leader/17).',
                'Invalid relationship - when 1st $e in 040 is equal to rda, then 336 must be present.',
                'Invalid code in indicator 1 in 655.',
                'Invalid relationship - when indicator 2 in 655 is equal to 7, then $2 in 655 must be present.',
                '1st $0 in 655 is in the wrong format.',
            ],
            'fixedFieldErrors': [
                {
                    'tag': '000',
                    'errorLevel': 'SEVERE',
                    'message': 'Invalid code in Encoding Level (Leader/17).',
                }
            ],
            'variableFieldErrors': [
                {
                    'tag': '040',
                    'errorLevel': 'MINOR',
                    'message': 'Invalid relationship - when 1st $e in 040 is equal to rda, then 336 must be present.',
                },
                {
                    'tag': '040',
                    'errorLevel': 'MINOR',
                    'message': 'Invalid relationship - when 1st $e in 040 is equal to rda, then 338 must be present.',
                },
                {
                    'tag': '245',
                    'errorLevel': 'MINOR',
                    'message': 'Invalid relationship - when $h in 245 is present, then $e in 040 must not be equal to rda.',
                },
                {
                    'tag': '655',
                    'errorLevel': 'MINOR',
                    'message': 'Invalid relationship - when indicator 2 in 655 is equal to 7, then $2 in 655 must be present.',
                },
                {
                    'tag': '655',
                    'errorLevel': 'MINOR',
                    'message': '1st $0 in 655 is in the wrong format.',
                },
            ],
        },
    },
    {
        'uuid': '0cb6a16c-751e-5006-8a27-739eaa069917',
        'reason': 'Failed holdings_unset',
        'context': {
            'controlNumber': '1107750624',
            'requestedControlNumber': '1107750624',
            'institutionCode': '6617',
            'institutionSymbol': 'STF',
            'firstTimeUse': False,
            'success': False,
            'message': 'Unset Holdings Failed. Local bibliographic data (LBD) is attached to this record. To unset the holding, delete attached LBD first and try again.',
            'action': 'Unset Holdings',
        },
    },
    {
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
    },
    {
        "uuid": '7063655a-6196-416f-94e7-8d540e014805',
        "reason": 'Failed to update holdings',
        "context": {
            'controlNumber': '48780294',
            'requestedControlNumber': '48780294',
            'institutionCode': '158223',
            'institutionSymbol': 'RCJ',
            'firstTimeUse': False,
            'success': False,
            'message': 'Holding Updated Failed',
            'action': 'Set Holdings',
        },
    },
]


def test_filter_failures_task_no_failures(caplog):
    deleted_failures = {'S7Z': [], 'HIN': [], 'CASUM': [], 'RCJ': [], 'STF': []}

    match_failures = {'S7Z': [], 'HIN': [], 'CASUM': [], 'RCJ': [], 'STF': []}

    new_failures = {'S7Z': [], 'HIN': [], 'CASUM': [], 'RCJ': [], 'STF': []}

    update_failures = {'S7Z': [], 'HIN': [], 'CASUM': [], 'RCJ': [], 'STF': []}

    filtered_errors = filter_failures_task.function(
        delete=deleted_failures,
        match=match_failures,
        new=new_failures,
        update=update_failures,
    )

    assert filtered_errors["STF"] == {}
    assert filtered_errors["CASUM"] == {}

    assert (
        "Match failures: S7Z - 0, HIN - 0, CASUM - 0, RCJ - 0, STF - 0" in caplog.text
    )
    assert (
        "Update failures: S7Z - 0, HIN - 0, CASUM - 0, RCJ - 0, STF - 0" in caplog.text
    )


def test_filter_failures_task(caplog):
    deleted_failures = {
        'S7Z': [
            ERRORS[0],
        ],
        'HIN': [],
        'CASUM': [],
        'RCJ': [],
        'STF': [ERRORS[4]],
    }

    match_failures = {
        'S7Z': [],
        'HIN': [ERRORS[2]],
        'CASUM': [],
        'RCJ': [],
        'STF': [ERRORS[5]],
    }

    new_failures = {
        'S7Z': [ERRORS[1]],
        'HIN': [],
        'CASUM': [ERRORS[3]],
        'RCJ': [],
        'STF': [ERRORS[5]],
    }

    update_failures = {'S7Z': [], 'HIN': [], 'CASUM': [], 'RCJ': [ERRORS[6]], 'STF': []}

    filtered_errors = filter_failures_task.function(
        delete=deleted_failures,
        match=match_failures,
        new=new_failures,
        update=update_failures,
    )

    assert (
        filtered_errors['CASUM']["WorldcatRequest Error"][0]['context']
        == ERRORS[3]["context"]
    )
    assert filtered_errors['S7Z']["WorldcatRequest Error"][0] == {
        'uuid': '8c9447fa-0556-47cc-98af-c8d5e0d763fb',
        'context': "b'400 Client Error'",
    }
    assert len(filtered_errors['STF']['Failed to update holdings after match']) == 2


def test_holdings_set_errors_task(tmp_path, mocker, mock_dag_run):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_reports.Variable.get",
        return_value="https://folio-stanford.edu",
    )

    failures = {
        "RCJ": {
            "Failed to update holdings": [
                {"uuid": ERRORS[6]["uuid"], "context": ERRORS[6]["context"]}
            ]
        },
        "STF": {},
    }

    reports = holdings_set_errors_task.function(
        airflow=tmp_path,
        date=datetime.datetime(2024, 8, 9, 12, 15, 13, 445),
        failures=failures,
        dag_run=mock_dag_run,
    )

    law_report = pathlib.Path(reports['RCJ'])
    law_report_html = BeautifulSoup(law_report.read_text(), 'html.parser')

    h1 = law_report_html.find('h1')
    assert h1.text.startswith("OCLC Holdings Set Errors on 09 August 2024 for RCJ")

    list_items_oclc_error = law_report_html.select('tbody > tr > td > ul > li')
    assert list_items_oclc_error[0].text == "Control Number: 48780294"
    assert list_items_oclc_error[4].text == "Success: False"
    assert list_items_oclc_error[5].text == 'Message: Holding Updated Failed'


def test_holdings_set_errors_match_task(tmp_path, mocker, mock_dag_run):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_reports.Variable.get",
        return_value="https://folio-stanford.edu",
    )

    hin_error = copy.deepcopy(ERRORS[5])
    hin_error['institutionCode'] = "567101"
    hin_error["institutionSymbol"] = "HIN"

    failures = {
        "STF": {
            "Failed to update holdings after match": [
                {"uuid": ERRORS[5]["uuid"], "context": ERRORS[5]["context"]}
            ]
        },
        "HIN": {
            "Failed to update holdings after match": [
                {"uuid": "cd010e25-a846-44b3-bf1b-28c2a128da15", "context": hin_error}
            ]
        },
    }

    reports = holdings_set_errors_task.function(
        airflow=tmp_path,
        date=datetime.datetime(2024, 8, 9, 12, 15, 13, 445),
        failures=failures,
        match=True,
        dag_run=mock_dag_run,
    )

    sul_report = pathlib.Path(reports['STF'])
    sul_report_html = BeautifulSoup(sul_report.read_text(), 'html.parser')

    h1 = sul_report_html.find("h1")
    assert h1.text.startswith(
        "OCLC Holdings Matched Set Errors on 09 August 2024 for STF"
    )

    dag_a = sul_report_html.select("p > a")
    assert dag_a[0].text == "DAG Run"

    hoover_report = pathlib.Path(reports['HIN'])
    hoover_report_html = BeautifulSoup(hoover_report.read_text(), 'html.parser')

    h2 = hoover_report_html.find("h2")
    assert h2.text.startswith(
        "FOLIO Instances that failed trying to set Holdings after successful Match"
    )


def test_holdings_unset_errors_task(tmp_path, mocker, mock_dag_run):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_reports.Variable.get",
        return_value="https://folio-stanford.edu",
    )

    failures = {
        "STF": {
            "Failed holdings_unset": [
                {"uuid": ERRORS[4]["uuid"], "context": ERRORS[4]["context"]}
            ]
        }
    }

    reports = holdings_unset_errors_task.function(
        airflow=tmp_path,
        date=datetime.datetime(2024, 8, 9, 12, 15, 13, 445),
        failures=failures,
        dag_run=mock_dag_run,
    )

    sul_report = pathlib.Path(reports['STF'])
    sul_report_html = BeautifulSoup(sul_report.read_text(), 'html.parser')

    h1 = sul_report_html.find("h1")
    assert h1.text == "OCLC Holdings Unset Errors on 09 August 2024 for STF"


def test_multiple_oclc_numbers_task(tmp_path, mocker, mock_task_instance, mock_dag_run):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_reports.Variable.get",
        return_value="https://folio-stanford.edu",
    )

    reports = multiple_oclc_numbers_task.function(
        airflow=tmp_path,
        date=datetime.datetime(2024, 7, 29, 14, 19, 34, 245),
        ti=mock_task_instance,
        dag_run=mock_dag_run,
    )

    sul_report = pathlib.Path(reports['STF'])
    lane_report = pathlib.Path(reports['CASUM'])

    sul_report_html = BeautifulSoup(sul_report.read_text(), 'html.parser')
    lane_report_html = BeautifulSoup(lane_report.read_text(), 'html.parser')

    h1_sul = sul_report_html.find('h1')
    assert h1_sul.text.startswith("Multiple OCLC Numbers on 29 July 2024 for STF")

    sul_li_instances = sul_report_html.select('ol > li')
    assert len(sul_li_instances) == 2

    instance_01_oclc_numbers = sul_li_instances[0].find_all("li")
    assert instance_01_oclc_numbers[0].text.startswith("276045320")
    assert instance_01_oclc_numbers[1].text.startswith("145453718")

    dag_anchor = lane_report_html.find("a")
    assert dag_anchor.text.startswith("DAG Run")
    assert dag_anchor.attrs['href'].startswith(
        "https://folio-airflow.edu/scheduled__2024-07-29"
    )
