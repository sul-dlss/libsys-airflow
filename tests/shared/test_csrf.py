import asyncio

import pytest

from airflow.api_fastapi.core_api.security import USER_INJECTED_BY_TRUSTED_MIDDLEWARE
from airflow.configuration import conf
from fastapi import Depends, FastAPI, Form, Request
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

from csrf_helpers import csrf_cookies, csrf_token_pair, token_from_cookie  # noqa

from libsys_airflow.plugins.shared.csrf import (
    CSRF_COOKIE_NAME,
    CSRF_FIELD_NAME,
    CSRF_HEADER_NAME,
    CSRF_MAX_AGE,
    CSRF_SIGNED_COOKIE_NAME,
    CSRFCookieMiddleware,
    binding_fingerprint,
    cookie_path,
    csrf_binding,
    csrf_field,
    csrf_protect,
    csrf_token,
    secret_key,
    signing_key,
)


class FakeUser:
    def __init__(self, user_id: str):
        self.user_id = user_id

    def get_id(self) -> str:
        return self.user_id

    def get_name(self) -> str:
        return self.user_id


class StampUserMiddleware(BaseHTTPMiddleware):
    """Stands in for Airflow's JWTRefreshMiddleware, which resolves the _token JWT and
    stamps the user on the request state before any mounted plugin app sees it."""

    def __init__(self, app, user_id: str):
        super().__init__(app)
        self.user_id = user_id

    async def dispatch(self, request: Request, call_next):
        request.state.user = FakeUser(self.user_id)
        request.state.user_authenticated_via = USER_INJECTED_BY_TRUSTED_MIDDLEWARE
        return await call_next(request)


def build_app(user_id: str | None = None) -> FastAPI:
    app = FastAPI()
    app.add_middleware(CSRFCookieMiddleware)
    if user_id is not None:
        # Added last, so it runs first and the CSRF middleware sees the stamped user.
        app.add_middleware(StampUserMiddleware, user_id=user_id)

    @app.get("/")
    def home(request: Request):
        return {"field": str(csrf_field(request))}

    @app.get("/binding")
    async def binding(request: Request):
        return {"binding": await csrf_binding(request)}

    @app.post("/create", dependencies=[Depends(csrf_protect)])
    def create(name: str = Form(default="")):  # noqa: B008
        return {"name": name}

    return app


def _request(cookies: dict | None = None) -> Request:
    headers = []
    if cookies:
        cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())
        headers.append((b"cookie", cookie_header.encode()))
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/create",
            "headers": headers,
        }
    )


@pytest.fixture
def csrf_app():
    return build_app()


def test_secret_key_comes_from_airflow_config():
    assert secret_key() == conf.get("api", "secret_key")
    assert secret_key(), "a secret key is required to sign CSRF tokens"


def test_cookie_path_from_airflow_base_url():
    # tests/conftest.py sets AIRFLOW__API__BASE_URL to http://localhost:8080
    assert cookie_path() == "/"


def test_generated_tokens_are_a_signed_pair():
    token, signed_token = csrf_token_pair()

    assert token != signed_token
    # itsdangerous signatures are <payload>.<timestamp>.<signature>
    assert signed_token.count(".") == 2
    assert csrf_token_pair()[0] != token


def test_signing_key_differs_per_identity():
    assert signing_key("alice") != signing_key("bob")
    assert signing_key("alice") == signing_key("alice")
    assert signing_key("") not in (signing_key("alice"), signing_key("bob"))


def test_binding_fingerprint_does_not_leak_the_signing_key():
    for user_id in ("", "alice"):
        assert binding_fingerprint(user_id) not in signing_key(user_id)
    assert binding_fingerprint("alice") != binding_fingerprint("bob")


def test_binding_is_empty_without_a_session():
    assert asyncio.run(csrf_binding(_request())) == ""


def test_binding_is_the_authenticated_user_id():
    response = TestClient(build_app(user_id="alice")).get("/binding")

    assert response.json() == {"binding": "alice"}


def test_csrf_token_reads_the_token_half_of_the_cookie():
    request = _request({CSRF_COOKIE_NAME: "abcdef0123456789.the-token"})

    assert csrf_token(request) == "the-token"


def test_csrf_token_is_empty_without_a_cookie():
    assert csrf_token(_request()) == ""


def test_csrf_field_renders_a_hidden_input():
    request = _request({CSRF_COOKIE_NAME: "abcdef0123456789.abc123"})

    expected = '<input type="hidden" name="csrf_token" value="abc123">'
    assert str(csrf_field(request)) == expected


def test_csrf_field_escapes_the_token():
    request = _request({CSRF_COOKIE_NAME: '0123456789abcdef."><script>'})

    assert "<script>" not in str(csrf_field(request))


def test_middleware_sets_both_cookies(csrf_app):
    response = TestClient(csrf_app).get("/")

    cookie = response.cookies[CSRF_COOKIE_NAME]
    fingerprint, _, token = cookie.partition(".")
    assert fingerprint == binding_fingerprint("")
    assert token and response.cookies[CSRF_SIGNED_COOKIE_NAME] != token
    expected = f'<input type="hidden" name="csrf_token" value="{token}">'
    assert response.json()["field"] == expected


def test_middleware_signed_cookie_is_httponly(csrf_app):
    response = TestClient(csrf_app).get("/")

    set_cookies = response.headers.get_list("set-cookie")
    signed = next(c for c in set_cookies if c.startswith(CSRF_SIGNED_COOKIE_NAME))
    plain = next(c for c in set_cookies if c.startswith(f"{CSRF_COOKIE_NAME}="))
    assert "HttpOnly" in signed
    assert "HttpOnly" not in plain
    assert f"Max-Age={CSRF_MAX_AGE}" in signed


def test_middleware_reuses_existing_cookies(csrf_app):
    cookies = csrf_cookies()
    client = TestClient(csrf_app, cookies=cookies)

    response = client.get("/")

    assert "set-cookie" not in response.headers
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    expected = f'<input type="hidden" name="csrf_token" value="{token}">'
    assert response.json()["field"] == expected


def test_middleware_reissues_when_only_one_cookie_is_present(csrf_app):
    cookies = csrf_cookies()
    client = TestClient(csrf_app, cookies={CSRF_COOKIE_NAME: cookies[CSRF_COOKIE_NAME]})

    response = client.get("/")

    assert response.cookies[CSRF_COOKIE_NAME] != cookies[CSRF_COOKIE_NAME]


def test_middleware_reissues_when_the_identity_changes():
    """A pair issued to one user is rolled as soon as somebody else is logged in."""
    alice_cookies = csrf_cookies("alice")
    client = TestClient(build_app(user_id="bob"), cookies=alice_cookies)

    response = client.get("/")

    assert response.cookies[CSRF_COOKIE_NAME] != alice_cookies[CSRF_COOKIE_NAME]
    assert response.cookies[CSRF_COOKIE_NAME].startswith(binding_fingerprint("bob"))


def test_post_accepts_the_token_from_the_form_field(csrf_app):
    client = TestClient(csrf_app)
    client.get("/")  # issues the cookies, as rendering the page would
    token = token_from_cookie(client.cookies[CSRF_COOKIE_NAME])

    response = client.post("/create", data={"name": "a cart", CSRF_FIELD_NAME: token})

    assert response.status_code == 200
    assert response.json() == {"name": "a cart"}


def test_post_accepts_the_token_from_a_multipart_form(csrf_app):
    client = TestClient(csrf_app)
    client.get("/")
    token = token_from_cookie(client.cookies[CSRF_COOKIE_NAME])

    response = client.post(
        "/create",
        data={"name": "a cart", CSRF_FIELD_NAME: token},
        files={"upload": ("barcodes.txt", b"12345\n", "text/plain")},
    )

    assert response.status_code == 200


def test_post_accepts_the_token_from_the_header(csrf_app):
    cookies = csrf_cookies()
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    client = TestClient(csrf_app, cookies=cookies)

    response = client.post(
        "/create", data={"name": "a cart"}, headers={CSRF_HEADER_NAME: token}
    )

    assert response.status_code == 200


def test_post_without_a_token_is_rejected(csrf_app):
    response = TestClient(csrf_app).post("/create", data={"name": "a cart"})

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF token missing or invalid"


def test_post_with_a_mismatched_token_is_rejected(csrf_app):
    client = TestClient(csrf_app)
    client.get("/")

    response = client.post(
        "/create", data={"name": "a cart", CSRF_FIELD_NAME: "not-the-token"}
    )

    assert response.status_code == 403


def test_post_without_the_signed_cookie_is_rejected(csrf_app):
    cookies = csrf_cookies()
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    client = TestClient(csrf_app, cookies={CSRF_COOKIE_NAME: cookies[CSRF_COOKIE_NAME]})

    response = client.post("/create", data={"name": "a cart", CSRF_FIELD_NAME: token})

    assert response.status_code == 403


def test_post_with_an_unsigned_forged_cookie_is_rejected(csrf_app):
    """A token an attacker can set as a cookie is useless without a valid signature."""
    client = TestClient(
        csrf_app,
        cookies={
            CSRF_COOKIE_NAME: f"{binding_fingerprint('')}.attacker-chosen",
            CSRF_SIGNED_COOKIE_NAME: "attacker-chosen",
        },
    )

    response = client.post(
        "/create", data={"name": "a cart", CSRF_FIELD_NAME: "attacker-chosen"}
    )

    assert response.status_code == 403


def test_post_with_a_tampered_signature_is_rejected(csrf_app):
    cookies = csrf_cookies()
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    cookies[CSRF_SIGNED_COOKIE_NAME] = cookies[CSRF_SIGNED_COOKIE_NAME][:-1] + "x"
    client = TestClient(csrf_app, cookies=cookies)

    response = client.post("/create", data={"name": "a cart", CSRF_FIELD_NAME: token})

    assert response.status_code == 403


def test_post_accepts_a_token_bound_to_the_same_user():
    cookies = csrf_cookies("alice")
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    client = TestClient(build_app(user_id="alice"), cookies=cookies)

    response = client.post("/create", data={"name": "a cart", CSRF_FIELD_NAME: token})

    assert response.status_code == 200


def test_post_rejects_a_token_bound_to_another_user():
    """The attack this binding exists for: an attacker mints a legitimate pair for
    themselves, overwrites the victim's CSRF cookies from a same-site host, and submits
    the matching token from a forged form."""
    attacker_cookies = csrf_cookies("mallory")
    token = token_from_cookie(attacker_cookies[CSRF_COOKIE_NAME])
    victim_client = TestClient(build_app(user_id="alice"), cookies=attacker_cookies)

    response = victim_client.post(
        "/create", data={"name": "a cart", CSRF_FIELD_NAME: token}
    )

    assert response.status_code == 403


def test_post_rejects_an_unbound_token_when_authenticated():
    cookies = csrf_cookies()
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    client = TestClient(build_app(user_id="alice"), cookies=cookies)

    response = client.post("/create", data={"name": "a cart", CSRF_FIELD_NAME: token})

    assert response.status_code == 403


def test_post_rejects_a_bound_token_when_unauthenticated(csrf_app):
    cookies = csrf_cookies("alice")
    token = token_from_cookie(cookies[CSRF_COOKIE_NAME])
    client = TestClient(csrf_app, cookies=cookies)

    response = client.post("/create", data={"name": "a cart", CSRF_FIELD_NAME: token})

    assert response.status_code == 403
