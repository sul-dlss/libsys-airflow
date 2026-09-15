"""
The ``external_views`` entries each plugin registers, and the invariants the apps depend
on.

Collect the entries from the plugin classes rather than listing them, so a plugin added
later is checked too. Pairs each view with the ``fastapi_apps`` entry of the same plugin
class, which is what ties a nav entry to the mount it points at.
"""

import importlib

from pathlib import Path

import pytest

from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.shared.nav import APPS

PLUGINS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "libsys_airflow" / "plugins"
)

# Airflow mounts its own apps under these, so a plugin cannot claim one.
# airflow.api_fastapi.app.RESERVED_URL_PREFIXES, copied rather than imported so that
# renaming it upstream shows up here as a stale list instead of an import error.
RESERVED_URL_PREFIXES = ["/api/v2", "/ui", "/execution", "/auth", "/pluginsv2"]


def _plugin_classes():
    classes = []
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
                classes.append(value)
    return classes


def _view_app_pairs():
    """Each ``external_views`` entry beside the ``fastapi_apps`` entry it points at."""
    pairs = []
    for plugin in _plugin_classes():
        views = getattr(plugin, "external_views", None) or []
        apps = getattr(plugin, "fastapi_apps", None) or []
        for view, app in zip(views, apps):
            pairs.append(pytest.param(view, app, id=view["name"]))
    return pairs


VIEW_APP_PAIRS = _view_app_pairs()
VIEWS = [pair.values[0] for pair in VIEW_APP_PAIRS]
URL_PREFIXES = [pair.values[1]["url_prefix"] for pair in VIEW_APP_PAIRS]


def test_views_were_collected():
    """Otherwise the parametrized tests below would pass by being empty."""
    assert len(VIEW_APP_PAIRS) == 12


@pytest.mark.parametrize("view,app", VIEW_APP_PAIRS)
def test_view_does_not_set_a_url_route(view, app):
    """
    A ``url_route`` makes Airflow render the app in an iframe sandboxed without
    ``allow-downloads``, ``allow-popups`` or ``allow-top-navigation``, and it never
    appends its own route splat to the iframe ``src``, so the browser URL cannot follow
    the user into the app and none of its pages can be bookmarked. Omitting it makes
    Airflow link to the app instead. See libsys_airflow/plugins/shared/nav.py.
    """
    assert "url_route" not in view


@pytest.mark.parametrize("view,app", VIEW_APP_PAIRS)
def test_view_is_fully_described(view, app):
    assert view["name"]
    # Airflow groups the nav by category; without one the entry lands at the top level.
    assert view["category"]


@pytest.mark.parametrize("view,app", VIEW_APP_PAIRS)
def test_view_href_is_its_app_mount_with_a_trailing_slash(view, app):
    """
    Starlette's Mount compiles ``url_prefix + "/{path:path}"``, so a bare ``/orafin``
    does not match the mount and Airflow's catch-all answers it with the whole UI. The
    href also has to name the app's own mount, which pins typos like the app being
    mounted at /circ_rule_tester while the nav points somewhere else.
    """
    assert view["href"] == f"{app['url_prefix']}/"
    assert view["href"].startswith("/")


def test_no_url_prefix_is_a_prefix_of_another():
    """
    Apache ``<Location>`` blocks in the Puppet repository authorize each app path against
    its own workgroup, and they match by prefix, so one app nested under another's prefix
    would be covered by the wrong workgroup.
    """
    nested = [
        (outer, inner)
        for outer in URL_PREFIXES
        for inner in URL_PREFIXES
        if outer != inner and inner.startswith(f"{outer}/")
    ]
    assert nested == []


def test_no_url_prefix_is_reserved_by_airflow():
    assert [
        prefix
        for prefix in URL_PREFIXES
        if any(
            prefix == reserved or prefix.startswith(f"{reserved}/")
            for reserved in RESERVED_URL_PREFIXES
        )
    ] == []


def test_nav_list_matches_the_registered_views():
    """
    shared.nav.APPS is written by hand so the nav partial does not have to import every
    plugin app. This is what keeps it honest when an app is added or renamed.
    """
    assert sorted((app.name, app.category, app.href) for app in APPS) == sorted(
        (view["name"], view["category"], view["href"]) for view in VIEWS
    )
