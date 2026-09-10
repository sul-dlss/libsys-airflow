"""
Authorization for the FastAPI plugin apps.

Airflow applies no access control to the ``fastapi_apps`` it mounts, so every plugin
route is open to anyone who knows its URL until an app guards itself. Hiding an app from
the nav is not a substitute: ``GET /api/v2/plugins`` gates the whole plugins menu on a
single ``AccessView.PLUGINS`` check and never consults the individual apps.

Apply it once per app rather than per route, so a route added later cannot forget it::

    from libsys_airflow.plugins.shared.auth import require_view_access

    app = FastAPI(dependencies=[Depends(require_view_access("Boundwith CSV Upload"))])

The view name is only a label, matched to the plugin's ``external_views`` entry by
convention. No auth manager can act on it: Keycloak pushes it as a ``resource_id`` claim
that no policy type can read, and ``SimpleAuthManager`` ignores it. So this establishes
that the caller is a signed-in user holding an Airflow role, not which plugins they may
use.
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
