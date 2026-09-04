from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.core_api.security import get_user

from csrf_helpers import csrf_test_client

TEST_USERNAME = "testuser"


def authenticate(app, username: str = TEST_USERNAME, role: str = "admin") -> None:
    """
    Satisfy the app's ``require_view_access`` dependency for every client built from it.

    The override is registered on the app object, which the test module holds at import,
    so plain ``TestClient(app)`` instances built later in a test are authenticated too.
    That matters for the tests that deliberately omit a CSRF token: authorization runs
    before the route's own dependencies, so without this they would fail with a 401 and
    never reach the CSRF check they are asserting on.

    Tests run under ``SimpleAuthManager``, whose ``is_authorized_custom_view`` only asks
    for the ``VIEWER`` role and ignores the view name, so a real user object is enough
    and nothing needs mocking. Pass ``role=None`` to exercise the unauthorized path.
    """
    app.dependency_overrides[get_user] = lambda: SimpleAuthManagerUser(
        username=username, role=role
    )


def authed_test_client(
    app, username: str = TEST_USERNAME, role: str = "admin", **kwargs
):
    """
    ``csrf_test_client`` plus authentication, for apps behind ``require_view_access``.

    Note the CSRF token is still bound to the empty identity. ``csrf_binding`` resolves
    the user from the request itself — ``request.state.user`` or the JWT cookie — not
    from FastAPI dependency overrides, so as far as the CSRF module is concerned an
    overridden request is unauthenticated.
    """
    authenticate(app, username=username, role=role)
    return csrf_test_client(app, **kwargs)
