import pathlib

from libsys_airflow.plugins.vendor.edi import invoice_count


def test_invoice_count():
    assert invoice_count(pathlib.Path('tests/vendor/inv574076.edi.txt')) == 1
