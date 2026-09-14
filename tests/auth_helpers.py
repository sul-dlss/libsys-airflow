import pytest

from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.core_api.security import get_user

TEST_USERNAME = "testuser"

# Every app ``authenticate`` has overridden and ``unauthenticate`` has not put back. The
# apps are module singletons, so tests/conftest.py drains this after each test rather
# than trusting each caller to undo itself.
_authenticated_apps: set = set()


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

    Safe to call from inside a test: the override is dropped again after it. Calling it
    at module level instead makes it outlive the teardown, and pytest runs that during
    collection, so the app would be authenticated for the whole session.
    """
    _authenticated_apps.add(app)
    app.dependency_overrides[get_user] = lambda: SimpleAuthManagerUser(
        username=username, role=role
    )


def unauthenticate(app) -> None:
    _authenticated_apps.discard(app)
    app.dependency_overrides.pop(get_user, None)


def unauthenticate_all() -> None:
    while _authenticated_apps:
        unauthenticate(next(iter(_authenticated_apps)))


def authenticated_app_fixture(app, username: str = TEST_USERNAME, role: str = "admin"):
    """
    Build an autouse fixture authenticating ``app`` for the tests in one module.

    Bind it at module level, as in ``authenticated = authenticated_app_fixture(app)``,
    alongside a plain ``csrf_test_client(app)``. Building the client at import is fine;
    authenticating at import is not, because the override would outlive the teardown
    that keeps it from reaching the tests asserting on anonymous requests.

    A ``csrf_test_client`` paired with this is still carrying a token bound to the empty
    identity. ``csrf_binding`` resolves the user from the request — ``request.state.user``
    or the JWT cookie — not from FastAPI dependency overrides, so as far as the CSRF
    module is concerned these requests are unauthenticated.
    """

    @pytest.fixture(autouse=True)
    def authenticated():
        authenticate(app, username=username, role=role)
        yield
        unauthenticate(app)

    return authenticated
