import pytest

from plugins.vendor.vendors import VendorManagementView

def test_vendor_management_view():
    vendor_management_app = VendorManagementView()
    assert vendor_management_app
