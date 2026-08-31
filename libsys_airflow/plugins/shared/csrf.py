"""
CSRF protection for the plugin FastAPI apps.

The Airflow 2 FAB webserver gave every Flask plugin app CSRF protection for free via
Flask-WTF's CSRFProtect, and templates only had to render ``{{ csrf_token() }}``. Airflow 3
mounts plugin apps as plain FastAPI sub-applications (``app.mount(url_prefix, subapp)``) and
neither the API server nor the configured auth manager provides an equivalent, so each app
has to protect its own unsafe routes. Because the auth manager authenticates browsers with
the ``_token`` JWT cookie, a forged cross-origin POST would otherwise arrive already
authenticated.

Token generation and validation are delegated to fastapi-csrf-protect, using its ``flexible``
variant so a token is accepted either from the hidden form field or from the ``X-CSRFToken``
header. The module around it is only glue, so that adopting CSRF protection in an app stays a
one-line change instead of the library's documented per-route ``generate_csrf_tokens`` /
``set_csrf_cookie`` wiring.

Two cookies are used. ``csrf_signed_token`` is the library's httponly, signed cookie and is the
one that is actually validated. ``csrf_token`` holds the matching unsigned token so a later page
render can reproduce the value the form has to submit; it is not a secret (the same value is
rendered into the page HTML), and a forged submission still has to be accompanied by a correctly
signed cookie, which cannot be produced without the server's secret key.

Tokens are also bound to the authenticated user: the signing key is derived from the user's id,
so a pair minted for one user fails verification for another. Without this, an attacker could
mint a legitimate pair for themselves, overwrite the victim's CSRF cookies from any other
``stanford.edu`` host (same-site for cookie purposes) and submit the matching token. Binding only
takes effect once the routes require authentication, which is a separate change; until then every
request is unauthenticated, resolves to an empty binding, and behaves as it did before.

Usage in an app::

    app = FastAPI()
    app.add_middleware(CSRFCookieMiddleware)

    @app.post("/create", dependencies=[Depends(csrf_protect)])
    def create(...):
        ...

and in its template, inside every ``<form method="post">``::

    {{ csrf_field(request) }}
"""

import hmac
import logging

from hashlib import sha256
from urllib.parse import urlsplit

from airflow.configuration import conf
from fastapi import HTTPException, Request
from fastapi_csrf_protect.exceptions import CsrfProtectError
from fastapi_csrf_protect.flexible import CsrfProtect
from markupsafe import Markup
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

CSRF_COOKIE_NAME = "csrf_token"
CSRF_SIGNED_COOKIE_NAME = "csrf_signed_token"
CSRF_FIELD_NAME = "csrf_token"
CSRF_HEADER_NAME = "X-CSRFToken"

# Airflow's own FAB config disables the CSRF time limit entirely so that a form left open
# never fails on submit. The library requires a finite age, so use a staff working day.
CSRF_MAX_AGE = 60 * 60 * 8

FORM_CONTENT_TYPES = ("application/x-www-form-urlencoded", "multipart/form-data")


def cookie_path() -> str:
    """
    Scope the cookies the same way Airflow scopes its own ``_token`` cookie, so the token
    is shared by every plugin app mounted under [api] base_url.
    """
    base_url = conf.get("api", "base_url", fallback="")
    if not base_url.endswith("/"):
        base_url += "/"
    return urlsplit(base_url).path or "/"


def cookie_is_secure() -> bool:
    return bool(conf.get("api", "ssl_cert", fallback=""))


def secret_key() -> str:
    """
    Reuse ``[api] secret_key``, which Airflow already requires to be identical across API
    server instances, rather than introducing another secret for operators to provision.
    """
    return conf.get("api", "secret_key", fallback="")


async def csrf_binding(request: Request) -> str:
    """
    The identity a token is issued to and validated against: the authenticated user's id, or
    an empty string when the request is not authenticated.

    The root app's ``JWTRefreshMiddleware`` already resolves the ``_token`` JWT and stamps the
    user on the request state, and mounted sub-apps share the same ASGI scope, so the common
    path costs nothing. Airflow is imported lazily because DAG modules reach this module
    through ``shared.utils`` and should not pull in the API server stack.
    """
    from airflow.api_fastapi.auth.managers.base_auth_manager import (
        COOKIE_NAME_JWT_TOKEN,
    )
    from airflow.api_fastapi.core_api.security import (
        USER_INJECTED_BY_TRUSTED_MIDDLEWARE,
        resolve_user_from_token,
    )

    user = getattr(request.state, "user", None)
    if user and getattr(request.state, "user_authenticated_via", None) is (
        USER_INJECTED_BY_TRUSTED_MIDDLEWARE
    ):
        return user.get_id()

    jwt_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
    if not jwt_token:
        return ""
    try:
        return (await resolve_user_from_token(jwt_token)).get_id()
    except HTTPException:
        # An expired or otherwise unusable session is simply unbound. Rejecting the request
        # is the auth dependency's job, not this module's.
        return ""


def signing_key(binding: str) -> str:
    """
    Per-identity signing key, so a token minted for one user fails signature verification for
    another. An empty binding yields a stable unbound key, which is the behavior of an
    unauthenticated app.
    """
    return hmac.new(secret_key().encode(), binding.encode(), sha256).hexdigest()


def binding_fingerprint(binding: str) -> str:
    """
    Short public marker of which identity the cookie pair was issued to, so the middleware can
    reissue after a login, logout or user switch. Derived separately from ``signing_key`` so
    that nothing in the readable cookie is derived from the signing key itself.
    """
    message = f"csrf-binding:{binding}".encode()
    return hmac.new(secret_key().encode(), message, sha256).hexdigest()[:16]


@CsrfProtect.load_config
def csrf_settings():
    return [
        ("secret_key", secret_key()),
        ("cookie_key", CSRF_SIGNED_COOKIE_NAME),
        ("cookie_path", cookie_path()),
        ("cookie_samesite", "lax"),
        ("cookie_secure", cookie_is_secure()),
        ("httponly", True),
        ("header_name", CSRF_HEADER_NAME),
        ("token_key", CSRF_FIELD_NAME),
        ("max_age", CSRF_MAX_AGE),
    ]


async def csrf_protect(request: Request) -> None:
    """
    FastAPI dependency for unsafe routes, rejecting the request with a 403 when the submitted
    token is missing or does not match the signed cookie.
    """
    content_type = request.headers.get("content-type", "")
    if content_type.startswith(FORM_CONTENT_TYPES):
        # Populating request._form makes the library read the token from the parsed form
        # rather than from its raw-body fallback, which cannot handle multipart uploads.
        await request.form()
    key = signing_key(await csrf_binding(request))
    try:
        await CsrfProtect().validate_csrf(request, secret_key=key)
    except CsrfProtectError as error:
        logger.warning(
            "Rejecting %s %s: %s", request.method, request.url.path, error.message
        )
        raise HTTPException(
            status_code=403, detail="CSRF token missing or invalid"
        ) from error


def csrf_token(request: Request) -> str:
    """
    Raw token value, for JavaScript that has to send it itself. The cookie stores the binding
    fingerprint alongside the token; only the token half is ever submitted.
    """
    state_token = getattr(request.state, "csrf_token", None)
    if state_token:
        return state_token
    _, _, token = request.cookies.get(CSRF_COOKIE_NAME, "").partition(".")
    return token


def csrf_field(request: Request) -> Markup:
    """
    Hidden input for a form, registered as a Jinja global by
    libsys_airflow.plugins.shared.utils.plugin_templates.

    Markup wraps a constant template and the values are substituted with Markup.format,
    which escapes them, rather than interpolating them into the string first.
    """
    return Markup('<input type="hidden" name="{}" value="{}">').format(
        CSRF_FIELD_NAME, csrf_token(request)
    )


class CSRFCookieMiddleware(BaseHTTPMiddleware):
    """
    Issues the token pair, minted with the signing key of the authenticated user so it is only
    valid for them. Makes the unsigned token available to templates as request.state.csrf_token
    and sets both cookies on the response when the request did not already carry a pair issued
    to the same identity. The request body is never touched, so file uploads are unaffected.
    """

    async def dispatch(self, request: Request, call_next):
        binding = await csrf_binding(request)
        fingerprint = binding_fingerprint(binding)

        cookie_fingerprint, _, token = request.cookies.get(
            CSRF_COOKIE_NAME, ""
        ).partition(".")
        signed_token = None
        if (
            not token
            or cookie_fingerprint != fingerprint
            or not request.cookies.get(CSRF_SIGNED_COOKIE_NAME)
        ):
            token, signed_token = CsrfProtect().generate_csrf_tokens(
                secret_key=signing_key(binding)
            )
        request.state.csrf_token = token

        response = await call_next(request)

        if signed_token:
            CsrfProtect().set_csrf_cookie(signed_token, response)
            # Readable by the template rendering the hidden field, which is what the
            # double-submit pattern requires; the token is not a credential.
            response.set_cookie(
                CSRF_COOKIE_NAME,
                f"{fingerprint}.{token}",
                max_age=CSRF_MAX_AGE,
                path=cookie_path(),
                secure=cookie_is_secure(),
                httponly=False,
                samesite="lax",
            )
        return response
