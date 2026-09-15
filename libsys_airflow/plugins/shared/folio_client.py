"""
FOLIO clients for DAGs and plugin apps.
"""

import threading
from datetime import datetime, timezone

import httpx
import jwt

from airflow.sdk import Variable
from folioclient import FolioClient
from folioclient._httpx import FolioAuth

_UNUSABLE = (
    "The user's FOLIO token has expired or been rejected. Trigger the DAG again from "
    "the plugin app; re-authenticating as the service account would misattribute the "
    "work in FOLIO."
)


class UserTokenUnusable(Exception):
    """The signed-in user's token cannot be used, so the caller must not proceed."""


def folio_client(**kwargs):
    client = kwargs.get("client")

    if client is None:
        client = FolioClient(
            Variable.get("OKAPI_URL", "http://example:9130"),
            "sul",
            Variable.get("FOLIO_USER", "nausername"),
            Variable.get("FOLIO_PASSWORD", "napassword"),
        )

    return client


def token_expires_at(token: str) -> datetime:
    """The ``exp`` of a Keycloak access token. Verifying its signature is FOLIO's job."""
    claims = jwt.decode(token, options={"verify_signature": False})
    return datetime.fromtimestamp(claims["exp"], tz=timezone.utc)


def folio_client_for_user(token: str) -> FolioClient:
    """
    A ``FolioClient`` acting as the user ``token`` belongs to.
    """
    try:
        expires_at = token_expires_at(token)
    except (jwt.PyJWTError, KeyError) as error:
        # Deliberately not echoing the token into the message or the logs.
        raise UserTokenUnusable(
            f"The user's FOLIO token could not be read: {error}"
        ) from error

    if expires_at <= datetime.now(tz=timezone.utc):
        raise UserTokenUnusable(_UNUSABLE)

    return _UserTokenFolioClient(Variable.get("OKAPI_URL"), "sul", token, expires_at)


class _UserTokenAuth(FolioAuth):
    """
    Holds a token handed to us instead of logging in with a username and password.

    folioclient re-authenticates from its stored credentials as a token nears expiry,
    and again whenever FOLIO answers 401, which here would silently switch a run to
    ``FOLIO_USER`` partway through. Both auth methods raise instead.
    """

    def __init__(self, params, token: str, expires_at: datetime):
        # Deliberately not calling super().__init__, which authenticates immediately.
        self._params = params
        self._tenant_id = params.tenant_id
        self._base_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._token = FolioAuth._Token(
            auth_token=token,
            refresh_token=None,
            expires_at=expires_at,
            refresh_token_expires_at=None,
            cookies=httpx.Cookies({"folioAccessToken": token}),
        )
        self._lock = threading.RLock()

    def _do_sync_auth(self):
        raise UserTokenUnusable(_UNUSABLE)

    async def _do_async_auth(self):
        raise UserTokenUnusable(_UNUSABLE)


class _UserTokenFolioClient(FolioClient):
    """A ``FolioClient`` whose identity comes from a token rather than a login."""

    def __init__(
        self, gateway_url: str, tenant_id: str, token: str, expires_at: datetime
    ):
        self._user_token = token
        self._user_token_expires_at = expires_at
        # Empty credentials: ``login`` installs the token and never uses them.
        super().__init__(gateway_url, tenant_id, "", "")

    def login(self) -> None:
        """Install the token. Called by ``FolioClient.__init__`` in place of a login."""
        if hasattr(self, "folio_auth"):
            # A re-login. folioclient's auth-error retry calls this before retrying a
            # 403, which for us would mean continuing as somebody else.
            raise UserTokenUnusable(_UNUSABLE)

        self.folio_auth = _UserTokenAuth(
            self.folio_parameters, self._user_token, self._user_token_expires_at
        )

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Tear down without FOLIO's ``authn/logout``, which the base class calls and which
        would end the user's own FOLIO session, not just this client's.
        """
        client = getattr(self, "httpx_client", None)
        if client is not None and not client.is_closed:
            client.close()

        self._cleanup_folio_parameters()
        self._cleanup_folio_auth()
        self.is_closed = True

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        client = getattr(self, "async_httpx_client", None)
        if client is not None and not client.is_closed:
            await client.aclose()

        self.__exit__(exc_type, exc_value, traceback)
