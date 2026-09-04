"""
Authorization for the FastAPI plugin apps.

Airflow mounts a plugin's ``fastapi_apps`` as sub-applications and applies no access
control of its own — ``is_authorized_custom_view`` only governs whether the matching
``external_views`` entry appears in the navigation, so without a dependency here the
routes are reachable by anyone who knows the URL.

Apply it once per app rather than per route, so a route added later cannot forget it::

    from libsys_airflow.plugins.shared.auth import require_view_access

    app = FastAPI(dependencies=[Depends(require_view_access("Boundwith CSV Upload"))])

The view name must match the plugin's ``external_views`` entry, since that is the name
Airflow itself passes when deciding whether to show the menu item. Keeping the two in
sync means the nav and the routes agree on who has access.

Two things are worth knowing about that name. Only Keycloak looks at it, and it cannot
currently act on it: every view shares a single ``Custom`` resource with the name pushed
as a ``resource_id`` claim, and no Keycloak policy type reads a pushed claim without a
server-side script deployment. ``SimpleAuthManager`` ignores both the name and the
method, asking only for the ``VIEWER`` role. So what this dependency establishes is that
the caller is a signed-in user holding an Airflow role, not which plugins they may use.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.core_api.security import GetUserDep
from fastapi import HTTPException, Request, status

if TYPE_CHECKING:
    # Only a Literal when type checking; at runtime the name is bound to an enum, so
    # importing it for real would annotate these with the wrong thing.
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod

# Airflow's ResourceMethod values that an HTTP verb can map onto. Anything unexpected is
# treated as POST: of the methods a plugin route might use it is the least permissive
# thing to demand, so an unknown verb cannot accidentally be checked as a read.
_METHODS: "dict[str, ResourceMethod]" = {
    "GET": "GET",
    "HEAD": "GET",
    "OPTIONS": "GET",
    "PUT": "PUT",
    "DELETE": "DELETE",
}


def resource_method(request: Request) -> "ResourceMethod":
    return _METHODS.get(request.method.upper(), "POST")


def require_view_access(view_name: str) -> Callable[[Request, GetUserDep], None]:
    """
    FastAPI dependency rejecting requests from users without access to ``view_name``.

    Unauthenticated requests fail with a 401 raised by Airflow's own ``get_user``;
    authenticated but unauthorized ones fail with a 403.
    """

    def inner(request: Request, user: GetUserDep) -> None:
        if not get_auth_manager().is_authorized_custom_view(
            method=resource_method(request),
            resource_name=view_name,
            user=user,
        ):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Forbidden")

    return inner
