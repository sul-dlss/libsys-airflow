from fastapi.testclient import TestClient
from fastapi_csrf_protect.flexible import CsrfProtect

from libsys_airflow.plugins.shared.csrf import (
    CSRF_COOKIE_NAME,
    CSRF_HEADER_NAME,
    CSRF_SIGNED_COOKIE_NAME,
    binding_fingerprint,
    signing_key,
)


def csrf_token_pair(user_id: str = "") -> tuple[str, str]:
    """
    The unsigned token a form submits and the signed token that validates it, minted for the
    given identity. The default empty identity is an unauthenticated request.
    """
    return CsrfProtect().generate_csrf_tokens(secret_key=signing_key(user_id))


def token_from_cookie(cookie_value: str) -> str:
    """
    The token half of the csrf_token cookie, which also carries a binding fingerprint.
    This is the value a form submits.
    """
    _, _, token = cookie_value.partition(".")
    return token


def csrf_cookies(user_id: str = "") -> dict[str, str]:
    token, signed_token = csrf_token_pair(user_id)
    return {
        CSRF_COOKIE_NAME: f"{binding_fingerprint(user_id)}.{token}",
        CSRF_SIGNED_COOKIE_NAME: signed_token,
    }


def csrf_test_client(app, user_id: str = "", **kwargs) -> TestClient:
    """
    TestClient that presents a valid CSRF token on every request, so tests that
    exercise a POST route's own behavior don't each have to supply one. Use a plain
    TestClient (or drop the cookies) to exercise the rejection path.
    """
    cookies = {**csrf_cookies(user_id), **(kwargs.pop("cookies", None) or {})}
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    headers = {CSRF_HEADER_NAME: token, **(kwargs.pop("headers", None) or {})}
    return TestClient(app, cookies=cookies, headers=headers, **kwargs)
