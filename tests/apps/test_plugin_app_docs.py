"""
Every plugin app must have FastAPI's automatic docs endpoints disabled.

``require_view_access`` is installed as an app-level dependency, which FastAPI copies
into ``APIRoute``s only. The endpoints ``FastAPI.setup()`` adds for the OpenAPI schema and
its browsers are plain Starlette routes, so they are not covered, and Airflow does not
cover for it either: plugin apps are mounted outside the API server's protected prefixes.
Collect the apps from the plugin classes rather than listing them, so an app added later
is checked too.
"""

import importlib

from pathlib import Path

import pytest

from airflow.plugins_manager import AirflowPlugin
from fastapi.testclient import TestClient

PLUGINS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "libsys_airflow" / "plugins"
)

DOCS_PATHS = ["/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"]


def _plugin_apps():
    apps = []
    for main in sorted(PLUGINS_DIR.glob("*/main.py")):
        module = importlib.import_module(
            f"libsys_airflow.plugins.{main.parent.name}.main"
        )
        for value in vars(module).values():
            if (
                isinstance(value, type)
                and issubclass(value, AirflowPlugin)
                and value is not AirflowPlugin
            ):
                for entry in getattr(value, "fastapi_apps", None) or []:
                    apps.append(pytest.param(entry["app"], id=entry["name"]))
    return apps


PLUGIN_APPS = _plugin_apps()


def test_plugin_apps_were_collected():
    """Otherwise the parametrized tests below would pass by being empty."""
    assert PLUGIN_APPS


@pytest.mark.parametrize("app", PLUGIN_APPS)
def test_plugin_app_has_no_docs_routes(app):
    assert app.openapi_url is None
    assert not {route.path for route in app.routes} & set(DOCS_PATHS)


@pytest.mark.parametrize("app", PLUGIN_APPS)
def test_plugin_app_docs_are_not_served_anonymously(app):
    """
    Not simply a 404 everywhere: an app whose own routes include a root-level path
    parameter now matches ``/docs`` itself, and answers 401 like any other route.

    Anonymity depends on nothing having authenticated these singleton apps earlier in
    the session, which ``_unauthenticate_plugin_apps`` in tests/conftest.py guarantees.
    """
    client = TestClient(app)

    for path in DOCS_PATHS:
        assert client.get(path).status_code != 200
