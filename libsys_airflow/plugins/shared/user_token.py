"""
Passes the signed-in user's FOLIO token to a DAG run out of band.

The token cannot travel in the run conf: the `User` role has GET and LIST on `Dag`, so
every signed-in user could read anyone else's live token from the run details. A Variable
is readable only by `Op` and above, encrypted at rest, and the `token` in its key makes
Airflow mask it in the UI. The run conf carries just the key.
"""

import uuid

_PREFIX = "folio_user_token_"


def store_user_token(token: str) -> str:
    """Stash ``token``, returning the key to put in the run conf. Called by a plugin app."""
    # Not airflow.sdk's Variable, whose set() talks to the task supervisor that only
    # exists in a worker. Plugin apps run in the API server, which has the database.
    from airflow.models import Variable

    key = f"{_PREFIX}{uuid.uuid4().hex}"
    Variable.set(key, token, description="Short-lived FOLIO token for one DAG run.")
    return key


def read_user_token(key: str) -> str | None:
    """The stashed token, or None if it is gone. Called from a task."""
    from airflow.sdk import Variable

    return Variable.get(key, default=None)


def discard_user_token(key: str) -> None:
    """Called from a task once the run no longer needs to act as the user."""
    from airflow.sdk import Variable

    Variable.delete(key)
