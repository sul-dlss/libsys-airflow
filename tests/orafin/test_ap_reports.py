import pytest  # noqa

from airflow.operators.bash import BashOperator

from libsys_airflow.plugins.orafin.reports import (
    ap_server_options,
    extract_rows,
    filter_files,
    find_reports,
    retrieve_reports,
    remove_reports,
)

report = [
    "SupplierNumber,SupplierName,PaymentNumber,PaymentDate,PaymentAmount,InvoiceNum,InvoiceDate,InvoiceAmt,AmountPaid,PoNumber",
    "910092,ALVARADO, JANET MARY,2384230,09/19/2023,50000,ALVARADOJM09052023 10103,08/23/2021,50000,50000,",
    "001470,AMERICAN MATHEMATICAL SOCIETY,3098367,09/02/2023,11405.42,2991432678 379587,08/03/2023,11405.42,11405.42,",
]


def test_extract_rows(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"

    with existing_csv.open('w+') as fo:
        for row in report:
            fo.write(f"{row}\n")

    invoices = extract_rows(str(existing_csv))

    assert len(invoices) == 2
    assert invoices[0]["SupplierName"] == "ALVARADO, JANET MARY"
    assert invoices[0]["PaymentDate"] == "09/19/2023"
    assert invoices[0]["InvoiceAmt"] == "50000"
    assert invoices[1]["SupplierNumber"] == "001470"
    assert invoices[1]["InvoiceNum"] == "2991432678 379587"
    assert invoices[1]["AmountPaid"] == "11405.42"
    assert invoices[1]["PoNumber"] == ""


def test_extract_rows_empty_file(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"
    with existing_csv.open('w+') as fo:
        fo.write(f"{report[0]}\n")

    assert existing_csv.exists()
    invoices = extract_rows(str(existing_csv))
    assert len(invoices) == 0
    assert existing_csv.exists() is False


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


def test_find_reports():
    bash_operator = find_reports()
    assert isinstance(bash_operator, BashOperator)
    assert bash_operator.bash_command.startswith("ssh")
    assert ap_server_options[1] in bash_operator.bash_command
    assert bash_operator.bash_command.endswith(
        "ls -m /home/of_aplib/OF1_PRD/outbound/data/*.csv"
    )


def test_retrieve_reports():
    partial_bash_operator = retrieve_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert bash_command.startswith("scp -i")
    assert ap_server_options[-1] in bash_command
    assert bash_command.endswith("/opt/airflow/orafin-files/reports/")


def test_remove_reports():
    partial_bash_operator = remove_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert bash_command.startswith("ssh")
    assert ap_server_options[0] in bash_command
    assert bash_command.endswith("rm /home/of_aplib/OF1_PRD/outbound/data/$file_name")
