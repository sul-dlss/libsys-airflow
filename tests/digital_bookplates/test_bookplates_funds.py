import datetime
import pytest  # noqa

from airflow.providers.postgres.hooks.postgres import PostgresHook
from pytest_mock_resources import create_sqlite_fixture, Rows

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from libsys_airflow.plugins.digital_bookplates.bookplates import (
    add_979_marc_tags,
    bookplate_funds_polines,
    launch_digital_bookplate_979_dag,
    launch_poll_for_979_dags,
    trigger_digital_bookplate_979_task,
    _new_bookplates,
)

rows = Rows(
    DigitalBookplate(
        id=1,
        created=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        updated=datetime.datetime(2024, 9, 11, 13, 15, 0, 733715),
        druid="kp761xz4568",
        fund_name="ASHENR",
        fund_uuid="b8932bcd-7498-4f7e-a598-de9010561e42",
        image_filename="dp698zx8237_00_0001.jp2",
        title="Ruth Geraldine Ashen Memorial Book Fund",
    ),
    DigitalBookplate(
        id=2,
        created=datetime.datetime(2024, 9, 12, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 12, 17, 16, 15, 986798),
        druid="gc698jf6425",
        image_filename="gc698jf6425_00_0001.jp2",
        fund_name="RHOADES",
        fund_uuid="06220dd4-7d6e-4e5b-986d-5fca21d856ca",
        title="John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
    ),
    DigitalBookplate(
        id=3,
        created=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        updated=datetime.datetime(2024, 9, 13, 17, 16, 15, 986798),
        druid="ab123xy4567",
        fund_name=None,
        fund_uuid=None,
        image_filename="ab123xy4567_00_0001.jp2",
        title="Alfred E. Newman Magazine Fund for Humor Studies",
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def mock_dag_bag(mocker):
    def mock_get_dag(dag_id: str):
        return mocker.MagicMock()

    dag_bag = mocker.MagicMock()
    dag_bag.get_dag = mock_get_dag
    return dag_bag


@pytest.fixture
def mock_invoice_lines():
    return [
        {
            "id": "cb0baa2d-7dd7-4986-8dc7-4909bbc18ce6",
            "fundDistributions": [
                {
                    "code": "ASHENR-SUL",
                    "encumbrance": "cfb59e90-014a-4860-9f5e-bdcfbf1a9f6f",
                    "fundId": "b8932bcd-7498-4f7e-a598-de9010561e42",
                    "distributionType": "percentage",
                    "value": 100.0,
                }
            ],
            "invoiceId": "29f339e3-dfdc-43e4-9442-eb817fdfb069",
            "invoiceLineNumber": "10",
            "invoiceLineStatus": "Paid",
            "poLineId": "be0af62c-665e-4178-ae13-e3250d89bcc6",
        },
        {
            "id": "5c6cffcf-1951-47c9-817f-145cbe931dea",
            "invoiceId": "2dcebfd3-82b0-429d-afbb-dff743602bea",
            "invoiceLineNumber": "29",
            "invoiceLineStatus": "Paid",
            "poLineId": "d55342ce-0a33-4aa2-87c6-5ad6e1a12b75",
        },
        {
            "id": "abc123",
            "fundDistributions": [
                {
                    "code": "RHOADES-SUL",
                    "fundId": "06220dd4-7d6e-4e5b-986d-5fca21d856ca",
                    "distributionType": "percentage",
                    "value": 50.0,
                },
                {
                    "code": "NONE",
                    "fundId": "abc1234",
                    "distributionType": "percentage",
                    "value": 50.0,
                },
                {
                    "code": "ASHENR-SUL",
                    "encumbrance": "cfb59e90-014a-4860-9f5e-bdcfbf1a9f6f",
                    "fundId": "b8932bcd-7498-4f7e-a598-de9010561e42",
                    "distributionType": "percentage",
                    "value": 100.0,
                },
            ],
            "invoiceId": "def456",
            "invoiceLineNumber": "2",
            "invoiceLineStatus": "Paid",
            "poLineId": "5513c3d7-7c6b-45ea-a875-09798b368873",
        },
    ]


@pytest.fixture
def mock_new_funds():
    return [
        {
            "druid": "ef919yq2614",
            "failure": None,
            "fund_name": "KELP",
            "fund_uuid": "f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4",
            "title": "The Kelp Foundation Fund",
            "image_filename": "ef919yq2614_00_0001.jp2",
        }
    ]


@pytest.fixture
def mock_new_bookplates():
    return {
        "f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4": {
            "fund_name": "KELP",
            "druid": "ef919yq2614",
            "image_filename": "ef919yq2614_00_0001.jp2",
            "title": "The Kelp Foundation Fund",
        }
    }


@pytest.fixture
def mock_bookplate_funds_polines():
    return [
        {
            "bookplate_metadata": {
                "fund_name": "ASHENR",
                "druid": "kp761xz4568",
                "image_filename": "dp698zx8237_00_0001.jp2",
                "title": "Ruth Geraldine Ashen Memorial Book Fund",
            },
            "poline_id": "be0af62c-665e-4178-ae13-e3250d89bcc6",
        },
        {
            "bookplate_metadata": {
                "fund_name": "RHOADES",
                "druid": "gc698jf6425",
                "image_filename": "gc698jf6425_00_0001.jp2",
                "title": "John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
            },
            "poline_id": "5513c3d7-7c6b-45ea-a875-09798b368873",
        },
        {
            "bookplate_metadata": {
                "fund_name": "ASHENR",
                "druid": "kp761xz4568",
                "image_filename": "dp698zx8237_00_0001.jp2",
                "title": "Ruth Geraldine Ashen Memorial Book Fund",
            },
            "poline_id": "5513c3d7-7c6b-45ea-a875-09798b368873",
        },
    ]


@pytest.fixture
def pg_hook(mocker, engine) -> PostgresHook:
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


def test_bookplate_funds_polines(
    pg_hook, mock_invoice_lines, mock_bookplate_funds_polines, caplog
):
    new_funds = []
    bookplates_polines = bookplate_funds_polines.function(
        invoice_lines=mock_invoice_lines, params={"funds": new_funds}
    )

    assert bookplates_polines == mock_bookplate_funds_polines
    assert "Getting bookplates data from the table" in caplog.text


def test_new_bookplates(mock_new_funds, mock_new_bookplates):
    new_bookplates = _new_bookplates(mock_new_funds)
    assert new_bookplates == mock_new_bookplates


def test_new_bookplate_funds_polines(
    mock_invoice_lines, mock_new_funds, mock_new_bookplates, caplog
):
    mock_invoice_lines.append(
        {
            "id": "cb0baa2d-7dd7-4986-8dc7-4909bbc18ce6",
            "fundDistributions": [
                {
                    "code": "KELP",
                    "fundId": "f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4",
                }
            ],
            "invoiceId": "29f339e3-dfdc-43e4-9442-eb817fdfb069",
            "poLineId": "def456",
        }
    )
    bookplates_polines = bookplate_funds_polines.function(
        invoice_lines=mock_invoice_lines, params={"funds": mock_new_funds}
    )
    assert len(bookplates_polines) == 1
    assert (
        bookplates_polines[0]["bookplate_metadata"]
        == mock_new_bookplates["f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4"]
    )
    assert bookplates_polines[0]["poline_id"] == "def456"
    assert "Getting bookplates data from list of new funds" in caplog.text


def test_add_979_marc_tags():
    druid_instances = {
        "b8932bcd-7498-4f7e-a598-de9010561e42": [
            {
                "druid": "kp761xz4568",
                "fund_name": "ASHENR",
                "image_filename": "dp698zx8237_00_0001.jp2",
                "title": "Ruth Geraldine Ashen Memorial Book Fund",
            },
        ],
        "06220dd4-7d6e-4e5b-986d-5fca21d856ca": [
            {
                "druid": "gc698jf6425",
                "fund_name": None,
                "image_filename": "gc698jf6425_00_0001.jp2",
                "title": "John Skylstead and Carmel Cole Rhoades Fund for California History and the History of the North American West",
            },
        ],
    }

    marc_979_tags = add_979_marc_tags.function(druid_instances)
    assert len(marc_979_tags["979"]) == 2
    assert len(marc_979_tags["979"][0]["subfields"]) == 4
    assert len(marc_979_tags["979"][1]["subfields"]) == 4
    assert marc_979_tags["979"][0]["subfields"][1]["b"] == "druid:kp761xz4568"
    assert marc_979_tags["979"][1]["subfields"][2]["c"] == "gc698jf6425_00_0001.jp2"
    assert marc_979_tags["979"][1]["subfields"][0]["f"] == "gc698jf6425"


def test_launch_digital_bookplate_979_dag(mocker, mock_dag_bag, caplog):
    dag_bag = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.DagBag",
        return_value=mock_dag_bag,
    )

    launch_digital_bookplate_979_dag(
        instance_uuid="01ae59b3-d7c6-4bf6-8097-02f9227932fa", funds=[{}]
    )

    assert dag_bag.called
    assert "Triggers 979 DAG with dag_id" in caplog.text


def test_launch_poll_for_979_dags(mocker, mock_dag_bag, caplog):
    dag_bag = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.DagBag",
        return_value=mock_dag_bag,
    )

    launch_poll_for_979_dags(dag_runs=['manual__2024-10-24:00:00:00'])

    assert dag_bag.called
    assert "Triggers polling DAG for 979 DAG runs" in caplog.text


def test_trigger_digital_bookplate_979_task(mocker, mock_dag_bag, caplog):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.DagBag",
        return_value=mock_dag_bag,
    )
    incoming_instances = [
        {},
        {
            'a855e551-47da-4621-9e05-5da512f526f7': [
                {
                    'fund_name': 'TANENBAUM',
                    'druid': 'yv459xj8957',
                    'image_filename': 'yv459xj8957_00_0001.jp2',
                    'title': 'The Mary M. Tanenbaum Chinese Art Fund',
                }
            ]
        },
        {},
    ]
    dag_run_ids = trigger_digital_bookplate_979_task.function(
        instances=incoming_instances
    )

    assert "Total incoming instances 3" in caplog.text
    assert len(dag_run_ids) == 1
