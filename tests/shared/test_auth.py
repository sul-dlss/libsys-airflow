import pytest

from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.core_api.security import get_user
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from libsys_airflow.plugins.shared.auth import (
    require_view_access,
    resource_method,
    session_expired,
)


# Named to dodge S107; S105 is ignored for tests.
FAKE_ACCESS_TOKEN = "a.keycloak.jwt"


class KeycloakUser:
    """A stand-in for KeycloakAuthManagerUser, which carries the Keycloak access JWT."""

    def __init__(self, access_token: str = FAKE_ACCESS_TOKEN):
        self.access_token = access_token

    def get_id(self) -> str:
        return "testuser"


def keycloak_manager(mocker, active: bool):
    """An auth manager whose Keycloak reports the access token active or otherwise."""
    manager = mocker.MagicMock()
    client = manager.get_keycloak_client.return_value
    client.introspect.return_value = {"active": active}
    return manager


@pytest.fixture
def app():
    """A stand-in for a plugin app, guarded the way the real ones are."""
    application = FastAPI(dependencies=[Depends(require_view_access("Test View"))])

    @application.get("/")
    def home():
        return {"ok": True}

    @application.post("/create")
    def create():
        return {"created": True}

    return application


def authenticate_as(app, role):
    app.dependency_overrides[get_user] = lambda: SimpleAuthManagerUser(
        username="testuser", role=role
    )


@pytest.mark.parametrize(
    "http_method, expected",
    [
        ("GET", "GET"),
        ("HEAD", "GET"),
        ("OPTIONS", "GET"),
        ("PUT", "PUT"),
        ("DELETE", "DELETE"),
        ("POST", "POST"),
        ("PATCH", "POST"),
    ],
)
def test_resource_method(http_method, expected):
    request = Request({"type": "http", "method": http_method, "headers": []})
    assert resource_method(request) == expected


def test_unauthenticated_request_is_rejected(app):
    """No session at all, which is the case the dependency exists to cover."""
    response = TestClient(app).get("/")

    assert response.status_code == 401


def test_unauthenticated_post_is_rejected(app):
    response = TestClient(app).post("/create")

    assert response.status_code == 401


def test_authorized_user_is_allowed(app):
    authenticate_as(app, "admin")

    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_user_without_keycloak_tokens_is_rejected(app):
    """
    KeycloakAuthManager.get_user_from_token returns None, rather than raising, for a
    browser whose Keycloak access token has expired and whose refresh token Keycloak no
    longer accepts. A 401 sends them back through login; passing the None on reached the
    auth manager as an AttributeError and a 500.
    """
    app.dependency_overrides[get_user] = lambda: None

    response = TestClient(app).get("/")

    assert response.status_code == 401
    assert response.json()["detail"] == "Not authenticated"


def test_unauthorized_user_is_forbidden(app):
    """A signed-in user the auth manager declines: 403 rather than 401."""
    authenticate_as(app, None)

    response = TestClient(app).get("/")

    assert response.status_code == 403
    assert response.json()["detail"] == "Forbidden"


def test_forgotten_session_is_rejected_as_unauthenticated(app, mocker):
    """
    Keycloak declines a user whose session it has forgotten exactly as it declines one
    who lacks the permission. Introspection tells them apart, and a forgotten session
    earns the 401 that sends the user back through login rather than a dead end 403.
    """
    manager = keycloak_manager(mocker, active=False)
    manager.is_authorized_custom_view.return_value = False
    mocker.patch(
        "libsys_airflow.plugins.shared.auth.get_auth_manager", return_value=manager
    )
    app.dependency_overrides[get_user] = lambda: KeycloakUser()

    response = TestClient(app).get("/")

    assert response.status_code == 401


def test_live_session_without_permission_stays_forbidden(app, mocker):
    """The denial Keycloak meant: redirecting to login would loop without changing it."""
    manager = keycloak_manager(mocker, active=True)
    manager.is_authorized_custom_view.return_value = False
    mocker.patch(
        "libsys_airflow.plugins.shared.auth.get_auth_manager", return_value=manager
    )
    app.dependency_overrides[get_user] = lambda: KeycloakUser()

    response = TestClient(app).get("/")

    assert response.status_code == 403


def test_session_expired_is_false_when_keycloak_cannot_be_reached(mocker):
    """An unreachable Keycloak must not turn a 403 into a login loop, or into a 500."""
    manager = keycloak_manager(mocker, active=False)
    manager.get_keycloak_client.side_effect = OSError("connection refused")
    mocker.patch(
        "libsys_airflow.plugins.shared.auth.get_auth_manager", return_value=manager
    )

    assert session_expired(KeycloakUser()) is False


def test_session_expired_is_false_under_a_non_keycloak_auth_manager(mocker):
    """An auth manager with no Keycloak to ask, which is what SimpleAuthManager is."""
    mocker.patch(
        "libsys_airflow.plugins.shared.auth.get_auth_manager", return_value=object()
    )

    assert session_expired(KeycloakUser()) is False


def test_session_expired_is_false_for_a_user_with_no_keycloak_token(mocker):
    mocker.patch(
        "libsys_airflow.plugins.shared.auth.get_auth_manager",
        return_value=keycloak_manager(mocker, active=False),
    )

    assert session_expired(SimpleAuthManagerUser(username="x", role="admin")) is False


def test_view_name_is_passed_to_the_auth_manager(app, mocker):
    """
    SimpleAuthManager ignores the view name, so assert on the call rather than the
    outcome. Under Keycloak this name is what a per-view policy keys off.
    """
    authenticate_as(app, "admin")
    manager = mocker.MagicMock()
    manager.is_authorized_custom_view.return_value = True
    mocker.patch(
        "libsys_airflow.plugins.shared.auth.get_auth_manager", return_value=manager
    )

    TestClient(app).post("/create")

    assert manager.is_authorized_custom_view.call_args.kwargs["resource_name"] == (
        "Test View"
    )
    assert manager.is_authorized_custom_view.call_args.kwargs["method"] == "POST"
