import datetime
import pathlib

import pytest

from bs4 import BeautifulSoup
from libsys_airflow.plugins.data_exports import oclc_reports


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


def test_multiple_oclc_numbers_task(tmp_path, mocker, mock_task_instance, mock_dag_run):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_reports.Variable.get",
        return_value="https://folio-stanford.edu",
    )

    reports = oclc_reports.multiple_oclc_numbers_task.function(
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
