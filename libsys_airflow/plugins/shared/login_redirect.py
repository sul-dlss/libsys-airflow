"""
Sends an unauthenticated browser to Keycloak, and back to the page it asked for.

Airflow's own UI handles a 401 in JavaScript by redirecting to the auth manager's login
route. The plugin apps are server-rendered, so without this a bookmarked or emailed
plugin URL just prints ``{"detail":"Not authenticated"}``, and reloading never recovers.
That happens routinely rather than only after a browser restart: Airflow's ``_token``
cookie is set with no ``max_age``, and ``JWTRefreshMiddleware`` clears it outright once
it expires.

Two halves, sharing the ``_libsys_next`` cookie:

``install_login_redirect`` turns a 401 into a redirect to Keycloak. Installed per app,
alongside ``require_view_access``.

``LoginReturnMiddleware`` sends the user back afterwards. The auth manager's login
callback always redirects to ``[api] base_url`` and offers no way to ask for anywhere
else, so the destination has to be carried across the round trip ourselves. It acts on
both legs of that trip: at ``/auth/login`` it binds the stashed destination to the OAuth
state of the login being started, and at the callback it honours the destination only if
that state comes back. Registered once as a root middleware by
``libsys_airflow.plugins.auth_redirect``.
"""

import secrets

from urllib.parse import quote, unquote

from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import HTTP_401_UNAUTHORIZED

from fastapi import FastAPI, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import RedirectResponse, Response

from libsys_airflow.plugins.shared.csrf import cookie_is_secure, cookie_path
from libsys_airflow.plugins.shared.nav import APPS

NEXT_COOKIE_NAME = "_libsys_next"

# Separates the OAuth state from the destination in the cookie. Legal in a cookie value
# per http.cookies, and absent from the percent-encoded path and the state alike.
NEXT_BINDING_SEPARATOR = "|"

# Long enough for a Keycloak round trip including an identity provider prompt. An
# abandoned attempt is discarded by the next login rather than by this expiry, which
# only bounds how long an untouched cookie sits in the browser.
NEXT_MAX_AGE = 60 * 10

# Illegal in a header value, so a Location built from one is rejected by the HTTP layer.
CONTROL_CHARACTERS = frozenset(chr(code) for code in (*range(0x20), 0x7F))


def wanted_path(request: Request) -> str:
    """
    Where to return the caller, percent-encoded as a ``Location`` header needs.

    Taken from the raw ASGI path where the server offers one, so a report filename that
    needed encoding survives the round trip, and so the value stays ASCII.
    """
    raw = request.scope.get("raw_path")
    path = raw.decode("ascii").split("?", 1)[0] if raw else quote(request.url.path)
    return f"{path}?{request.url.query}" if request.url.query else path


def is_returnable(path: str) -> bool:
    """
    Whether ``path`` is safe to redirect to after login.

    Only paths inside a plugin app qualify. The value reaches us in a cookie, so it is
    checked again on the way out rather than trusted from having been written on the way
    in: a protocol-relative ``//host`` would leave the site, and ``..`` would escape the
    app's own prefix, and so the Apache location block that guards it.
    """
    if not path.startswith("/") or path.startswith("//"):
        return False
    if not path.isascii() or CONTROL_CHARACTERS.intersection(path):
        return False
    if ".." in unquote(path):
        return False
    return any(path.startswith(f"{app.url_prefix}/") for app in APPS)


def install_login_redirect(app: FastAPI) -> None:
    """
    Answer a 401 with a redirect to Keycloak when the caller is a browser.

    Registered for Starlette's ``HTTPException`` rather than FastAPI's subclass of it,
    because FastAPI keys its own default handler on the Starlette class; registering on
    the base catches both and leaves everything else to that default.
    """

    async def handler(request: Request, exc: StarletteHTTPException) -> Response:
        if not _should_redirect(request, exc):
            return await http_exception_handler(request, exc)

        from airflow.api_fastapi.app import get_auth_manager

        path = wanted_path(request)
        # next_url is the convention SimpleAuthManager already honours, so local
        # development returns to the page without needing LoginReturnMiddleware at all.
        # KeycloakAuthManager ignores it, which is what the middleware is for; if the
        # provider ever grows support, this starts working with no change here.
        response = RedirectResponse(url=get_auth_manager().get_url_login(next_url=path))
        # Percent-encoded so the value holds only characters http.cookies considers
        # legal. Left raw, it would be stored as a quoted string, which round trips
        # today only because Starlette's parser happens to strip the quotes.
        _stash_next(response, quote(path, safe=""))
        return response

    # Starlette types handlers as taking a bare Exception; this one is registered for
    # StarletteHTTPException only, so it annotates what it actually receives.
    app.add_exception_handler(StarletteHTTPException, handler)  # type: ignore[arg-type]


def _should_redirect(request: Request, exc: StarletteHTTPException) -> bool:
    """
    Only unauthenticated page loads.

    A 403 is deliberately excluded: it means Keycloak authenticated the caller and then
    denied them, so sending them back to log in would loop through the identity provider
    without changing the outcome. A non-GET is excluded because the redirect would
    discard its body, and a client that did not ask for HTML is better served by the
    original error than by a login page.
    """
    if exc.status_code != HTTP_401_UNAUTHORIZED:
        return False
    if request.method != "GET":
        return False
    return "text/html" in request.headers.get("accept", "")


class LoginReturnMiddleware:
    """
    Carry the stashed destination across the login round trip, and redirect to it when
    Keycloak hands the user back.

    Plain ASGI rather than ``BaseHTTPMiddleware``, following ``SessionScopeMiddleware``:
    this is a root middleware, so it sees every request to the API server including the
    plugin apps' file downloads, and ``BaseHTTPMiddleware`` would wrap all of them in
    its own streaming machinery. Anything that is not a login route is passed straight
    through untouched, and so is a login route with no cookie to act on.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path")
        if path == _login_path():
            rewrite = _bind_to_login
        elif path == _login_callback_path():
            rewrite = _return_after_login
        else:
            await self.app(scope, receive, send)
            return

        stashed = Request(scope).cookies.get(NEXT_COOKIE_NAME)
        if not stashed:
            await self.app(scope, receive, send)
            return

        async def send_rewritten(message):
            if message["type"] == "http.response.start":
                message = rewrite(message, stashed, scope)
            await send(message)

        await self.app(scope, receive, send_rewritten)


def _bind_to_login(start_message: dict, stashed: str, scope: dict) -> dict:
    """
    Tie the stashed destination to the login now being started, or discard it.

    A cookie that is already bound belongs to an earlier login. Reaching ``/auth/login``
    again means a second login is under way, so the earlier destination is dropped
    rather than carried into it: otherwise an attempt the user abandoned would hijack
    wherever they meant to go when they next signed in.

    The binding is the provider's own OAuth ``state``, read from the ``Set-Cookie`` it
    just wrote. Its ``login_callback`` refuses a request whose ``state`` query parameter
    does not match that cookie, so a state arriving at the callback is authentic and
    identifies one specific round trip.

    A login response carrying no state is left alone. That is SimpleAuthManager in local
    development, which honours the ``next_url`` the 401 handler already passes and needs
    no help from here.
    """
    state = _oauth_state(start_message)
    if state is None:
        return start_message
    if NEXT_BINDING_SEPARATOR in stashed:
        return _with_next_cookie(start_message, None)
    return _with_next_cookie(start_message, f"{state}{NEXT_BINDING_SEPARATOR}{stashed}")


def _return_after_login(start_message: dict, stashed: str, scope: dict) -> dict:
    """
    Point a finished login at the stashed destination and drop the cookie that carried
    it.

    A response that is not a redirect is left alone, cookie included: authentication did
    not complete, so the destination is still worth keeping for the next attempt.

    The destination is only honoured when it was bound to the login now completing. An
    unbound value belongs to no login at all, and one bound to another means the login
    that stashed it was abandoned.
    """
    if not 300 <= start_message["status"] < 400:
        return start_message

    state, separator, encoded = stashed.partition(NEXT_BINDING_SEPARATOR)
    path = unquote(encoded)
    override = bool(separator) and _state_matches(state, scope) and is_returnable(path)

    headers = [
        (name, value)
        for name, value in start_message["headers"]
        if not (override and name.lower() == b"location")
    ]
    if override:
        headers.append((b"location", path.encode("latin-1")))

    return _with_next_cookie({**start_message, "headers": headers}, None)


def _state_matches(state: str, scope: dict) -> bool:
    """Whether this callback completes the login the destination was bound to."""
    returned = Request(scope).query_params.get("state", "")
    return bool(returned) and secrets.compare_digest(state, returned)


def _oauth_state(start_message: dict) -> str | None:
    """
    The OAuth state the login route just set, or None when it set none.

    Read from the response's own ``Set-Cookie`` headers rather than the request's, so
    it is the state of the login being started and not one left over from an earlier
    attempt.
    """
    from airflow.providers.keycloak.auth_manager.constants import (
        COOKIE_NAME_OAUTH_STATE,
    )

    prefix = f"{COOKIE_NAME_OAUTH_STATE}=".encode()
    for name, value in start_message["headers"]:
        if name.lower() == b"set-cookie" and value.startswith(prefix):
            return value[len(prefix) :].split(b";", 1)[0].decode("latin-1")
    return None


def _with_next_cookie(start_message: dict, value: str | None) -> dict:
    """``start_message`` with ``_libsys_next`` set to ``value``, or cleared if None."""
    headers = [
        (name, header_value)
        for name, header_value in start_message["headers"]
        if not (
            name.lower() == b"set-cookie"
            and header_value.startswith(f"{NEXT_COOKIE_NAME}=".encode())
        )
    ]
    headers.append((b"set-cookie", _next_cookie_header(value).encode("latin-1")))
    return {**start_message, "headers": headers}


def _stash_next(response: Response, value: str | None) -> None:
    """
    Set ``_libsys_next`` on ``response``, or clear it when ``value`` is None.

    The one place the cookie's attributes are stated, so the 401 handler, the binding
    step and the clearing step cannot drift apart -- a mismatched ``path`` in particular
    would set a second cookie instead of replacing the first.
    """
    if value is None:
        response.delete_cookie(
            NEXT_COOKIE_NAME,
            path=cookie_path(),
            httponly=True,
            secure=cookie_is_secure(),
            samesite="lax",
        )
        return
    response.set_cookie(
        NEXT_COOKIE_NAME,
        value,
        max_age=NEXT_MAX_AGE,
        path=cookie_path(),
        httponly=True,
        secure=cookie_is_secure(),
        samesite="lax",
    )


def _next_cookie_header(value: str | None) -> str:
    """``_stash_next`` as a raw ``Set-Cookie`` line, for the ASGI side to append."""
    response = Response()
    _stash_next(response, value)
    return response.headers["set-cookie"]


def _login_path() -> str:
    """Where the 401 handler sends the user, and where the provider mints its state."""
    from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX

    return f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login"


def _login_callback_path() -> str:
    """
    Where Keycloak returns the user. Built from Airflow's own prefix rather than a
    literal ``/auth``, which only holds while [api] base_url has no path of its own.
    """
    from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX

    return f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login_callback"
