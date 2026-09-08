import contextlib
import contextvars
import os
import threading
from uuid import uuid4

from sqlalchemy.orm import scoped_session, sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook


# The EngineWrapper allows for late creation of the PG connection.
# This allows for mocking out Session in testing.
class EngineWrapper:
    def __init__(self):
        self.engine = None

    def __getattr__(self, name):
        if self.engine is None:
            self.engine = PostgresHook("vendor_loads").get_sqlalchemy_engine(
                {"max_overflow": int(os.getenv("DATABASE_MAX_OVERFLOW", "10"))}
            )
        return getattr(self.engine, name)


engine = EngineWrapper()

# What ``Session()`` hands back, and what ``Session.remove()`` closes.
#
# scoped_session's default scope is the thread, which was the right unit under the Flask
# webserver because one request was handled start to finish on one thread. The FastAPI app
# is ASGI: middleware runs on the event loop thread, while the sync route functions run in
# anyio's worker threadpool. A thread-scoped ``Session.remove()`` called from middleware
# therefore clears the event loop thread's (nonexistent) session and orphans the worker
# thread's session along with the connection it checked out. anyio retires a worker thread
# after ten seconds idle, so ordinary click-around traffic gets a fresh thread, and so a
# fresh session and connection, for most requests; the orphans sit in an open transaction
# until the garbage collector happens to reclaim them. The count creeps up until the pool
# is exhausted and every request fails with "QueuePool limit of size 5 overflow 20
# reached, connection timed out".
#
# Scoping on a context variable instead makes the request the unit of reuse. The value is
# set before the request is dispatched, so it is inherited both by the child tasks
# BaseHTTPMiddleware spawns and by the worker thread anyio runs the route in, which copies
# the calling context. Route and middleware then agree on which session is the request's,
# so the one that checked out the connection is the one that gets closed. Outside a request
# the fallback keeps the previous per-thread behavior.
_session_scope: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "vendor_session_scope", default=None
)


def _current_scope() -> str:
    return _session_scope.get() or f"thread-{threading.get_ident()}"


Session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine),  # type: ignore
    scopefunc=_current_scope,
)


@contextlib.contextmanager
def session_scope():
    """
    Give the enclosed work a Session of its own, closed and discarded on the way out.
    """
    token = _session_scope.set(uuid4().hex)
    try:
        yield
    finally:
        Session.remove()
        _session_scope.reset(token)


class SessionScopeMiddleware:
    """
    Scope the session to one request and close it once the response has been sent.

    Plain ASGI rather than ``BaseHTTPMiddleware`` so that it runs in the request's own task,
    which is what lets the scope propagate to everything downstream, and so that the
    session is not closed while a response body is still streaming —
    ``BaseHTTPMiddleware.call_next`` returns as soon as the response starts, not when it
    has been sent.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        with session_scope():
            await self.app(scope, receive, send)
