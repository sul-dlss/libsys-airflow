from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from libsys_airflow.plugins.orafin.reports import (
    ap_server_options,
    extract_rows,
    filter_files,
    find_reports,
    retrieve_reports,
    remove_reports,
)

report = [
    "SupplierNumber\tSupplierName\tPaymentNumber\tPaymentDate\tPaymentAmount\tInvoiceNum\tInvoiceDate\tInvoiceAmt\tAmountPaid\tPoNumber",
    "910092\tALVARADO, JANET MARY\t2384230\t09/19/2023\t50000\tALVARADOJM09052023 10103\t08/23/2021\t50000\t50000\t",
    "001470\tAMERICAN MATHEMATICAL SOCIETY\t3098367\t09/02/2023\t11405.42\t2991432678 379587\t08/03/2023\t11405.42\t11405.42\t",
]


def test_extract_rows(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"

    with existing_csv.open('w+') as fo:
        for row in report:
            fo.write(f"{row}\n")

    invoices, dag_trigger = extract_rows(str(existing_csv))

    assert len(invoices) == 2
    assert invoices[0]["SupplierName"] == "ALVARADO, JANET MARY"
    assert invoices[0]["PaymentDate"] == "09/19/2023"
    assert invoices[0]["InvoiceAmt"] == "50000"
    assert invoices[1]["SupplierNumber"] == "001470"
    assert invoices[1]["InvoiceNum"] == "2991432678 379587"
    assert invoices[1]["AmountPaid"] == "11405.42"
    assert invoices[1]["PoNumber"] is None
    assert dag_trigger is None


def test_extract_rows_empty_file(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"
    with existing_csv.open('w+') as fo:
        fo.write(f"{report[0]}\n")

    assert existing_csv.exists()
    invoices, dag_trigger = extract_rows(str(existing_csv))
    assert len(invoices) == 0
    assert existing_csv.exists() is False
    assert dag_trigger is None


def test_extract_rows_large_file(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_112820231000.csv"
    with existing_csv.open('w+') as fo:
        fo.write(f"{report[0]}\n")
        for _ in range(1200):
            fo.write(f"{report[1]}\n")

    assert existing_csv.exists()
    invoices, dag_trigger = extract_rows(str(existing_csv))
    assert len(invoices) == 1_000
    assert isinstance(dag_trigger, TriggerDagRunOperator)
    next_csv = orafin_reports / "xxdl_ap_payment_112820231000_01.csv"
    assert next_csv.exists()


def test_filter_files(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"

    with existing_csv.open('w+') as fo:
        for row in report:
            fo.write(f"{row}\n")

    existing_reports, new_reports = filter_files(
        "xxdl_ap_payment_09282023161640.csv, xxdl_ap_payment_10212023177160.csv, xxdl_ap_payment.xml",
        airflow,
    )

    assert existing_reports == [{"file_name": "xxdl_ap_payment_09282023161640.csv"}]
    assert new_reports == [{"file_name": "xxdl_ap_payment_10212023177160.csv"}]


def test_find_reports(mocker):
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=True
    )
    bash_operator = find_reports()
    assert isinstance(bash_operator, BashOperator)
    assert bash_operator.bash_command.startswith("ssh")
    assert bash_operator.bash_command.endswith(
        "ls -m /home/of_aplib/OF1_PRD/outbound/data/*.csv | tr '\n' ' '"
    )


def test_retrieve_reports(mocker):
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=False
    )
    partial_bash_operator = retrieve_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert bash_command.startswith("scp -i")
    assert bash_command.endswith("/opt/airflow/orafin-files/reports/")
    assert "intxfer-uat.stanford.edu" in bash_command
    # Test production
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=True
    )
    partial_bash_operator = retrieve_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert "/home/of_aplib/OF1_PRD/outbound/data/$file_name" in bash_command


def test_remove_reports(mocker):
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=True
    )
    partial_bash_operator = remove_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert bash_command.startswith("ssh")
    assert ap_server_options[0] in bash_command
    assert bash_command.endswith("rm /home/of_aplib/OF1_PRD/outbound/data/$file_name")
