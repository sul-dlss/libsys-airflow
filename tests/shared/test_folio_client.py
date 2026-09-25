from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import jwt
import pytest

from airflow.sdk import Variable

from libsys_airflow.plugins.shared.folio_client import (
    UserTokenUnusable,
    folio_client_for_user,
    token_expires_at,
)


@pytest.fixture(autouse=True)
def okapi_url(monkeypatch):
    monkeypatch.setattr(Variable, "get", lambda key: "https://folio.example.edu")


AN_HOUR = timedelta(hours=1)


def user_token(expires_in=AN_HOUR):
    expires_at = datetime.now(tz=timezone.utc) + expires_in
    return jwt.encode({"exp": int(expires_at.timestamp())}, "not-checked-here")


def test_token_expires_at():
    expected = datetime.now(tz=timezone.utc) + timedelta(hours=1)

    assert abs((token_expires_at(user_token()) - expected).total_seconds()) < 2


def test_client_carries_the_users_token():
    token = user_token()

    client = folio_client_for_user(token)

    assert client.folio_auth._token.auth_token == token
    assert client.folio_auth._token.cookies["folioAccessToken"] == token
    assert client.folio_headers["x-okapi-token"] == token


def test_expired_token_is_refused():
    """The DAG fails rather than acting as the service account."""
    with pytest.raises(UserTokenUnusable, match="expired"):
        folio_client_for_user(user_token(expires_in=timedelta(minutes=-1)))


def test_unreadable_token_is_refused():
    with pytest.raises(UserTokenUnusable, match="could not be read"):
        folio_client_for_user("not-a-jwt")


def test_does_not_reauthenticate_as_the_service_account():
    client = folio_client_for_user(user_token())

    with pytest.raises(UserTokenUnusable):
        client.folio_auth._do_sync_auth()

    with pytest.raises(UserTokenUnusable):
        client.login()


@pytest.mark.parametrize("teardown", ["close", "logout", "__exit__"])
def test_teardown_does_not_log_the_user_out(teardown):
    client = folio_client_for_user(user_token())
    client.httpx_client = MagicMock(is_closed=False)

    if teardown == "__exit__":
        client.__exit__(None, None, None)
    else:
        getattr(client, teardown)()

    client.httpx_client.post.assert_not_called()
    client.httpx_client.close.assert_called_once()
    assert client.is_closed
