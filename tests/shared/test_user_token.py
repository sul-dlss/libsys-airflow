from unittest.mock import patch

from libsys_airflow.plugins.shared.user_token import (
    discard_user_token,
    read_user_token,
    store_user_token,
)


@patch("airflow.models.Variable.set")
def test_store_returns_a_key_airflow_will_mask(mock_set):
    key = store_user_token("a-keycloak-access-token")

    # "token" in the key is what makes Airflow hide the value in the UI.
    assert "token" in key
    assert mock_set.call_args[0][0] == key
    assert mock_set.call_args[0][1] == "a-keycloak-access-token"


@patch("airflow.models.Variable.set")
def test_keys_are_not_guessable(mock_set):
    assert store_user_token("a-token") != store_user_token("a-token")


@patch("airflow.sdk.Variable.get")
def test_read_a_discarded_token(mock_get):
    """A cleared run finds nothing, which the DAG reports rather than working around."""
    mock_get.return_value = None

    assert read_user_token("folio_user_token_gone") is None


@patch("airflow.sdk.Variable.delete")
def test_discard(mock_delete):
    discard_user_token("folio_user_token_abc123")

    mock_delete.assert_called_once_with("folio_user_token_abc123")
