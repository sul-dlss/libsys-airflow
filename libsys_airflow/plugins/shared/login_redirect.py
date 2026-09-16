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
else, so the destination has to be carried across the round trip ourselves. Registered
once as a root middleware by ``libsys_airflow.plugins.auth_redirect``.
"""

from urllib.parse import quote, unquote

from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import HTTP_401_UNAUTHORIZED

from fastapi import FastAPI, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import RedirectResponse, Response

from libsys_airflow.plugins.shared.csrf import cookie_is_secure, cookie_path
from libsys_airflow.plugins.shared.nav import APPS

NEXT_COOKIE_NAME = "_libsys_next"

# Long enough for a Keycloak round trip including an identity provider prompt, short
# enough that a stale destination cannot resurface in a later session.
NEXT_MAX_AGE = 60 * 10


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

    The dot-segment check is made against the decoded path, because browsers treat an
    encoded ``%2e%2e`` as a parent segment too and would normalise it away.
    """
    if not path.startswith("/") or path.startswith("//"):
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
        response.set_cookie(
            NEXT_COOKIE_NAME,
            # Percent-encoded so the value holds only characters http.cookies considers
            # legal. Left raw, it would be stored as a quoted string, which round trips
            # today only because Starlette's parser happens to strip the quotes.
            quote(path, safe=""),
            max_age=NEXT_MAX_AGE,
            path=cookie_path(),
            httponly=True,
            secure=cookie_is_secure(),
            samesite="lax",
        )
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
    Redirect to the stashed destination when Keycloak hands the user back.

    Plain ASGI rather than ``BaseHTTPMiddleware``, following ``SessionScopeMiddleware``:
    this is a root middleware, so it sees every request to the API server including the
    plugin apps' file downloads, and ``BaseHTTPMiddleware`` would wrap all of them in
    its own streaming machinery. Anything that is not the login callback is passed
    straight through untouched.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") != _login_callback_path():
            await self.app(scope, receive, send)
            return

        stashed = Request(scope).cookies.get(NEXT_COOKIE_NAME)
        if not stashed:
            await self.app(scope, receive, send)
            return
        path = unquote(stashed)

        async def send_with_destination(message):
            if message["type"] == "http.response.start":
                message = _redirect_to(message, path)
            await send(message)

        await self.app(scope, receive, send_with_destination)


def _redirect_to(start_message: dict, path: str) -> dict:
    """
    Point a finished login at ``path`` and drop the cookie that carried it.

    A response that is not a redirect is left alone: authentication did not complete, so
    the destination is still worth keeping for the next attempt.
    """
    if not 300 <= start_message["status"] < 400:
        return start_message

    override = is_returnable(path)
    headers = [
        (name, value)
        for name, value in start_message["headers"]
        if not (override and name.lower() == b"location")
    ]
    if override:
        headers.append((b"location", path.encode("latin-1")))
    headers.append((b"set-cookie", _expired_next_cookie().encode("latin-1")))

    return {**start_message, "headers": headers}


def _expired_next_cookie() -> str:
    """The ``Set-Cookie`` value that clears ``_libsys_next``, formatted by Starlette."""
    response = Response()
    response.delete_cookie(
        NEXT_COOKIE_NAME,
        path=cookie_path(),
        httponly=True,
        secure=cookie_is_secure(),
        samesite="lax",
    )
    return response.headers["set-cookie"]


def _login_callback_path() -> str:
    """
    Where Keycloak returns the user. Built from Airflow's own prefix rather than a
    literal ``/auth``, which only holds while [api] base_url has no path of its own.
    """
    from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX

    return f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login_callback"
