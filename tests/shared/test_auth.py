import pytest

from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.core_api.security import get_user
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from libsys_airflow.plugins.shared.auth import require_view_access, resource_method


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


def test_unauthorized_user_is_forbidden(app):
    """A signed-in user the auth manager declines: 403 rather than 401."""
    authenticate_as(app, None)

    response = TestClient(app).get("/")

    assert response.status_code == 403
    assert response.json()["detail"] == "Forbidden"


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
