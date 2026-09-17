"""
Sending an unauthenticated browser to Keycloak, and back again afterwards.

Boundwith stands in for every app, because the handler is installed identically by all
twelve and its views need no mocking. Deliberately no ``authenticated_app_fixture``
here: these tests are about anonymous requests, and ``tests/conftest.py``'s
``_unauthenticate_plugin_apps`` guarantees the singleton apps start that way.

``LoginReturnMiddleware`` is exercised at the bottom against a stand-in for the
provider's login routes, because it is a root middleware and never sees a plugin app's
own requests.
"""

from urllib.parse import quote, unquote

import pytest

from fastapi import FastAPI
from fastapi.responses import RedirectResponse, Response
from fastapi.testclient import TestClient

from libsys_airflow.plugins.boundwith.boundwith_view import app
from libsys_airflow.plugins.shared.login_redirect import (
    NEXT_BINDING_SEPARATOR,
    NEXT_COOKIE_NAME,
    LoginReturnMiddleware,
    _login_callback_path,
    _login_path,
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


def _next_cookie_line(response):
    """Our raw Set-Cookie line, before the client parses it, or None if we set none."""
    return next(
        (
            header
            for header in response.headers.get_list("set-cookie")
            if header.startswith(f"{NEXT_COOKIE_NAME}=")
        ),
        None,
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
    assert '"' not in _next_cookie_line(_client().get("/", headers=HTML))


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
        # Illegal in a Location header. wanted_path never produces these, but any
        # sibling stanford.edu host can set the cookie that carries them.
        "/orafin/\nX: y",  # header injection
        "/orafin/\r\nX: y",
        "/orafin/\x7f",
        "/orafin/é",  # latin-1 encodable, so it would not raise, but still not ours
        "/orafin/€",  # not latin-1 encodable, so encoding it raises
    ],
)
def test_unreturnable_paths(path):
    assert is_returnable(path) is False


STATE = "the_state_the_login_route_minted"
BASE_URL = "http://localhost:8080/"
WANTED = "/orafin/reports/x.csv"


def _login_routes(oauth_state=STATE, callback=None):
    """
    A stand-in for the provider's login routes, wrapped in the middleware.

    Mounted at the real ``AUTH_MANAGER_FASTAPI_APP_PREFIX`` paths, taken from the
    middleware's own helpers so the fixture cannot drift from what it matches on. The
    callback redirects to [api] base_url, as the provider's does, with no way to ask for
    anywhere else -- which is the whole reason the middleware exists.
    """
    from airflow.providers.keycloak.auth_manager.constants import (
        COOKIE_NAME_OAUTH_STATE,
    )

    routes = FastAPI()

    @routes.get(_login_path())
    def login():
        response = RedirectResponse("https://keycloak.example.edu/realms/sul/auth")
        if oauth_state is not None:
            response.set_cookie(COOKIE_NAME_OAUTH_STATE, oauth_state)
        return response

    @routes.get(_login_callback_path())
    def login_callback():
        return callback() if callback else RedirectResponse(BASE_URL, status_code=303)

    @routes.get("/orafin/")
    def a_plugin_page():
        return Response("orafin")

    return TestClient(LoginReturnMiddleware(routes), follow_redirects=False)


def _bound(state, path=WANTED):
    """The cookie value as ``_bind_to_login`` writes it."""
    return f"{state}{NEXT_BINDING_SEPARATOR}{quote(path, safe='')}"


def _with_cookie(client, value):
    client.cookies.set(NEXT_COOKIE_NAME, value)
    return client


def _cleared(response):
    return "Max-Age=0" in (_next_cookie_line(response) or "")


def test_the_login_binds_the_destination_to_its_own_state():
    """
    The state is read off the login response rather than the request, so it is the one
    this login just minted and not one left over from an attempt the user abandoned.
    """
    client = _with_cookie(_login_routes(), quote(WANTED, safe=""))

    response = client.get(_login_path())

    assert _next_cookie_line(response).startswith(
        f"{NEXT_COOKIE_NAME}={_bound(STATE)};"
    )


def test_the_bound_value_still_needs_no_cookie_quoting():
    """
    Both halves and the separator are legal in a cookie value, so http.cookies stores
    the value plainly rather than as a quoted string.
    """
    client = _with_cookie(_login_routes(), quote(WANTED, safe=""))

    assert '"' not in _next_cookie_line(client.get(_login_path()))


def test_a_second_login_discards_an_abandoned_destination():
    """
    An already bound cookie belongs to an earlier login. Without this, a plugin login
    the user abandoned would hijack wherever they meant to go when they next signed in.
    """
    client = _with_cookie(_login_routes(), _bound("a_state_from_an_earlier_attempt"))

    assert _cleared(client.get(_login_path()))


def test_a_login_that_mints_no_state_leaves_the_cookie_alone():
    """
    SimpleAuthManager in local development, which honours the next_url the 401 handler
    already passes and needs nothing from this middleware.
    """
    client = _with_cookie(_login_routes(oauth_state=None), quote(WANTED, safe=""))

    assert _next_cookie_line(client.get(_login_path())) is None


def test_the_destination_is_honoured_when_its_own_login_completes():
    client = _with_cookie(_login_routes(), _bound(STATE))

    response = client.get(f"{_login_callback_path()}?code=abc&state={STATE}")

    assert response.headers["location"] == WANTED
    assert _cleared(response)


def test_another_logins_callback_is_left_where_it_was_going():
    """
    The provider validates the returned state against its own cookie, so a state that
    reaches the callback is authentic and names one round trip. A destination bound to a
    different one is not this login's to redirect.
    """
    client = _with_cookie(_login_routes(), _bound("a_state_from_an_earlier_attempt"))

    response = client.get(f"{_login_callback_path()}?code=abc&state={STATE}")

    assert response.headers["location"] == BASE_URL
    assert _cleared(response)


def test_an_unbound_destination_is_not_honoured():
    """Never bound to a login, so there is nothing to say this one is its round trip."""
    client = _with_cookie(_login_routes(), quote(WANTED, safe=""))

    response = client.get(f"{_login_callback_path()}?code=abc&state={STATE}")

    assert response.headers["location"] == BASE_URL
    assert _cleared(response)


def test_an_unusable_destination_does_not_break_the_login():
    """
    A cookie any sibling stanford.edu host can set must not be able to wedge login. The
    header that clears it is built in the same list as the Location, so a path that
    could not be encoded used to take the clearing header down with it and leave every
    attempt failing for as long as the cookie lived.
    """
    client = _with_cookie(_login_routes(), _bound(STATE, "/orafin/\nX: y"))

    response = client.get(f"{_login_callback_path()}?code=abc&state={STATE}")

    assert response.status_code == 303
    assert response.headers["location"] == BASE_URL
    assert _cleared(response)


def test_a_callback_that_did_not_redirect_keeps_the_destination():
    """
    Authentication did not complete -- the provider answers a bad state with a 403 page
    -- so the destination is still worth keeping for the next attempt.
    """

    def a_rejected_callback():
        return Response("Invalid OAuth state parameter", status_code=403)

    client = _with_cookie(_login_routes(callback=a_rejected_callback), _bound(STATE))

    response = client.get(f"{_login_callback_path()}?code=abc&state={STATE}")

    assert response.status_code == 403
    assert _next_cookie_line(response) is None


def test_everything_else_passes_through_untouched():
    """
    A root middleware sees every request to the API server, including plugin page loads
    and file downloads, so it has to keep its hands off anything that is not a login.
    """
    client = _with_cookie(_login_routes(), _bound(STATE))

    response = client.get("/orafin/")

    assert response.status_code == 200
    assert response.text == "orafin"
    assert _next_cookie_line(response) is None
