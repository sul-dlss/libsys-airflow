from airflow.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory

from libsys_airflow.plugins.folio.apps.circ_rules_tester_view import CircRulesTester


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/folio/templates"

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False

    app.appbuilder.add_view(
        CircRulesTester,
        "CircRulesTester",
        category="Circ Rules Tests",
    )

    app.blueprints["CircRulesTester"].template_folder = templates_folder
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_circ_rules_tester_main_page(test_airflow_client):
    response = test_airflow_client.get("/circ_rule_tester/")

    assert response.status_code == 200

    title = response.html.find("h2")

    assert title.text == "FOLIO Circ Rules Tester"


def test_circ_rules_tester_reference_home(mocker, test_airflow_client):
    mocker.patch(
        'libsys_airflow.plugins.folio.apps.circ_rules_tester_view.folio_client'
    )

    response = test_airflow_client.get("/circ_rule_tester/reference")

    assert response.status_code == 200

    title = response.html.find("h2")

    assert title.text == "Reference Data"

    reference_list = response.html.find(id="ref-data-list")
    reference_list_items = reference_list.find_all("li")

    assert len(reference_list_items) == 4


mock_patron_groups = [
    {
        'group': 'graduate',
        'desc': 'Graduate Student',
        'id': 'ad0bc554-d5bc-463c-85d1-5562127ae91b',
        'metadata': {
            'createdDate': '2023-08-09T20:12:40.204+00:00',
            'updatedDate': '2026-01-28T22:40:29.039+00:00',
        },
    },
    {
        'group': 'staff',
        'desc': 'Staff Member',
        'id': '3684a786-6671-4268-8ed0-9db82ebca60b',
        'expirationOffsetInDays': 730,
        'metadata': {
            'createdDate': '2023-08-09T20:12:40.000+00:00',
            'updatedDate': '2026-01-28T22:40:29.040+00:00',
        },
    },
    {
        'group': 'undergrad',
        'desc': 'Undergraduate Student',
        'id': 'bdc2b6d4-5ceb-4a12-ab46-249b9a68473e',
        'metadata': {
            'createdDate': '2023-08-09T20:12:39.103+00:00',
            'updatedDate': '2026-01-28T22:40:29.140+00:00',
        },
    },
]


def test_circ_rules_tester_patron_group(mocker, test_airflow_client):
    mock_folio_client = mocker.MagicMock()
    mock_folio_client.folio_get = lambda *args, **kwargs: mock_patron_groups

    mocker.patch(
        'libsys_airflow.plugins.folio.apps.circ_rules_tester_view.folio_client',
        return_value=mock_folio_client,
    )

    response = test_airflow_client.get("/circ_rule_tester/reference/patron_group")

    title = response.html.find("h2")

    assert title.text == "Patron Groups"

    table_rows = response.html.find_all("tr")

    assert len(table_rows) == 4

    first_row_data = table_rows[1].find_all("td")

    assert first_row_data[0].text.startswith("graduate")
    assert first_row_data[1].text.startswith("Graduate Student")
    assert first_row_data[2].text.startswith("ad0bc554")
