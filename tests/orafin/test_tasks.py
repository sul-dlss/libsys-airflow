import pytest  # noqa

from libsys_airflow.plugins.orafin.tasks import consolidate_reports_task


def mock_xcom_pull(**kwargs):
    key = kwargs.get("key")

    output = []
    match key:
        case "existing_reports":
            output.append({"file_name": "xxdl_ap_payment_09282023161640.csv"})

        case "new_reports":
            output.append({"file_name": "xxdl_ap_payment_10212023177160.csv"})

    return output


def test_consolidate_reports_task(mocker):
    mock_task_instance = mocker
    mock_task_instance.xcom_pull = mock_xcom_pull
    all_reports = consolidate_reports_task.function(ti=mock_task_instance)
    assert len(all_reports) == 2
