from fastapi.testclient import TestClient
import pytest  # noqa

from libsys_airflow.plugins.folio.apps.circ_rules_tester_view import app

client = TestClient(app, follow_redirects=False)


def test_circ_rules_tester_main_page():
    response = client.get("/")

    assert response.status_code == 200
    assert "<h2>FOLIO Circ Rules Tester</h2>" in response.text


def test_circ_rules_tester_reference_home(mocker):
    mocker.patch(
        'libsys_airflow.plugins.folio.apps.circ_rules_tester_view.folio_client'
    )

    response = client.get("/reference")

    assert response.status_code == 200
    assert "<h2>Reference Data</h2>" in response.text

    assert response.text.count('<li><a href="reference/') == 4


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


def test_circ_rules_tester_patron_group(mocker):
    mock_folio_client = mocker.MagicMock()
    mock_folio_client.folio_get = lambda *args, **kwargs: mock_patron_groups

    mocker.patch(
        'libsys_airflow.plugins.folio.apps.circ_rules_tester_view.folio_client',
        return_value=mock_folio_client,
    )

    response = client.get("/reference/patron_group")

    assert response.status_code == 200
    assert "<h2>Patron Groups</h2>" in response.text

    assert response.text.count("<tr>") == 3

    assert "graduate" in response.text
    assert "Graduate Student" in response.text
    assert "ad0bc554" in response.text
