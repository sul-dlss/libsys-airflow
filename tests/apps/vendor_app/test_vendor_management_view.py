import pytest
from pytest_mock import MockerFixture
import requests

from libsys_airflow.plugins.vendor_app.vendors import VendorManagementView


def test_vendor_management_view():
    vendor_management_app = VendorManagementView()
    assert vendor_management_app
