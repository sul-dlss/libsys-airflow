"""
Sending an unauthenticated browser to Keycloak, and back again afterwards.

Boundwith stands in for every app, because the handler is installed identically by all
twelve and its views need no mocking. Deliberately no ``authenticated_app_fixture``
here: these tests are about anonymous requests, and ``tests/conftest.py``'s
``_unauthenticate_plugin_apps`` guarantees the singleton apps start that way.
"""

from urllib.parse import unquote

import pytest

from libsys_airflow.plugins.boundwith.boundwith_view import app
from libsys_airflow.plugins.shared.login_redirect import (
    NEXT_COOKIE_NAME,
    is_returnable,
)

from tests.auth_helpers import authenticate
from tests.csrf_helpers import csrf_test_client

HTML = {"accept": "text/html,application/xhtml+xml"}


def _client():
    """A client that shows us the redirect instead of chasing it to Keycloak."""
    return csrf_test_client(app, follow_redirects=False)


def _stashed(response):
    """The destination as LoginReturnMiddleware reads it back off the cookie."""
    return unquote(response.cookies[NEXT_COOKIE_NAME])


def _set_cookie_header(response):
    """Our raw Set-Cookie line, before the client parses it."""
    return next(
        header
        for header in response.headers.get_list("set-cookie")
        if header.startswith(f"{NEXT_COOKIE_NAME}=")
    )


def test_anonymous_page_load_is_sent_to_the_login_route():
    """
    Compared against the auth manager's own answer rather than a literal URL, because
    it differs per auth manager and, for SimpleAuthManager, per config.
    """
    from airflow.api_fastapi.app import get_auth_manager

    response = _client().get("/", headers=HTML)

    assert response.status_code in (302, 307)
    assert response.headers["location"] == get_auth_manager().get_url_login(
        next_url="/"
    )


def test_the_wanted_page_is_stashed_for_the_return_trip():
    response = _client().get("/", headers=HTML)

    assert _stashed(response) == "/"


def test_the_stashed_value_needs_no_cookie_quoting():
    """
    Percent-encoded, so http.cookies stores it plainly. Left raw it becomes a quoted
    string, which only round trips because Starlette's parser strips the quotes.
    """
    assert '"' not in _set_cookie_header(_client().get("/", headers=HTML))


def test_the_query_string_is_kept():
    """Report links carry their filename in the path, but filters use the query."""
    response = _client().get("/?message=hello", headers=HTML)

    assert _stashed(response) == "/?message=hello"


def test_a_json_client_still_gets_its_401():
    """An API caller is better served by the error than by a login page."""
    response = _client().get("/", headers={"accept": "application/json"})

    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}
    assert NEXT_COOKIE_NAME not in response.cookies


def test_a_post_is_not_redirected():
    """Redirecting would discard the body, so the caller gets the error instead."""
    response = _client().post("/create", data={"sunid": "testuser"}, headers=HTML)

    assert response.status_code == 401


def test_an_authorized_request_is_untouched():
    authenticate(app)

    response = _client().get("/", headers=HTML)

    assert response.status_code == 200


def test_an_unauthorized_user_is_not_redirected():
    """
    A 403 means Keycloak authenticated the caller and then denied them, so returning
    them to the login page would loop through the identity provider without ever
    changing the outcome.
    """
    authenticate(app, role=None)

    response = _client().get("/", headers=HTML)

    assert response.status_code == 403
    assert "location" not in response.headers


@pytest.mark.parametrize(
    "path",
    [
        "/boundwith/",
        "/vendor_management/interfaces/1",
        "/data_export_oclc_reports/missing_holdings/report.html",
        "/orafin/reports/x.csv?download=true",
    ],
)
def test_returnable_paths(path):
    assert is_returnable(path) is True


@pytest.mark.parametrize(
    "path",
    [
        "//evil.example.com",  # protocol-relative, leaves the site
        "/orafin/../../dags",  # escapes the app's Apache location block
        "/orafin/%2e%2e/%2e%2e/dags",  # the same, which a browser also normalises
        "/orafin/%2E%2E/dags",
        "/dags/boundwith/runs/1",  # outside the plugin apps
        "/etc/passwd",
        "/boundwith",  # the mount needs its trailing slash
        "",
        "relative/path",
    ],
)
def test_unreturnable_paths(path):
    assert is_returnable(path) is False
