import pytest

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.orders import instances_from_po_lines

mock_order_lines = {
    "8c91eb1c-4e32-44e8-bcce-b34850bdf3d5": {
        "instanceId": "e6803f0b-ed22-48d7-9895-60bea6826e93"
    },
    "84701022-43af-401e-bcea-fb5fbd8f49f3": {
        "instanceId": "e6803f0b-ed22-48d7-9895-60bea6826e93"
    },
    "9f7031df-d30b-40c2-955a-7d522c303a43": {"instanceId": None},
}


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        output = {}
        if str(args[0]).startswith("/orders/order-line"):
            poline_id = args[0].split("/")[-1]
            output = mock_order_lines[poline_id]
        return output

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    return mock_client


def test_instances_from_po_lines(mocker, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.folio.orders._folio_client",
        return_value=mock_folio_client,
    )

    incoming_data = [
        {
            "bookplate_metadata": {
                "druid": "gc698jf6425",
                "failure": None,
                "image_filename": "gc698jf6425_00_0001.jp2",
                "fund_name": "RHOADES",
                "title": "John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
            },
            "poline_id": "8c91eb1c-4e32-44e8-bcce-b34850bdf3d5",
        },
        {
            "bookplate_metadata": {
                "druid": "ef919yq2614",
                "failure": None,
                "fund_name": "KELP",
                "title": "The Kelp Foundation Fund",
                "image_filename": "ef919yq2614_00_0001.jp2",
            },
            "poline_id": "84701022-43af-401e-bcea-fb5fbd8f49f3",
        },
    ]
    instances_dict = instances_from_po_lines.function(incoming_data)
    assert instances_dict["e6803f0b-ed22-48d7-9895-60bea6826e93"][0] == {
        "druid": "gc698jf6425",
        "failure": None,
        "image_filename": "gc698jf6425_00_0001.jp2",
        "fund_name": "RHOADES",
        "title": "John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
    }
    assert (
        instances_dict["e6803f0b-ed22-48d7-9895-60bea6826e93"][1]["fund_name"] == "KELP"
    )


def test_instances_from_po_lines_no_instance(mocker, mock_folio_client, caplog):
    mocker.patch(
        "libsys_airflow.plugins.folio.orders._folio_client",
        return_value=mock_folio_client,
    )

    incoming_data = [
        {
            "bookplate_metadata": {
                "druid": "",
                "fund_name": "",
                "image_filename": "",
                "title": "",
            },
            "poline_id": "9f7031df-d30b-40c2-955a-7d522c303a43",
        }
    ]

    instances_dict = instances_from_po_lines.function(incoming_data)

    assert len(instances_dict) == 0
    assert (
        "PO Line 9f7031df-d30b-40c2-955a-7d522c303a43 not linked to a FOLIO Instance"
        in caplog.text
    )
