"""
The plugin apps, listed once, for the shared navigation partial `templates/_nav.html`.

Our plugins omit `url_route` so that Airflow links to the app instead of putting it in an iframe.
This allows for file downloads, following links, and working bookmarks. The plugin navigation
is built from the list of apps below.

``tests/apps/test_plugin_external_views.py`` checks this list against the plugin modules
themselves, so a new app that is not added here fails the suite.

Imports nothing outside the standard library: ``shared.utils`` imports this module, and
DAG modules reach ``shared.utils`` without the API server stack available.
"""

from typing import NamedTuple


class PluginApp(NamedTuple):
    name: str
    """The ``external_views`` name, which is also the ``require_view_access`` label."""

    category: str
    url_prefix: str

    @property
    def href(self) -> str:
        # The trailing slash is required, not cosmetic: Starlette's Mount compiles
        # url_prefix + "/{path:path}", so a bare "/orafin" misses the mount entirely
        # and Airflow's catch-all answers it with the whole UI.
        return f"{self.url_prefix}/"


APPS: tuple[PluginApp, ...] = (
    PluginApp("Boundwith CSV Upload", "FOLIO", "/boundwith"),
    PluginApp("Circ Rules Tester", "FOLIO", "/circ_rule_tester"),
    PluginApp("Data Export CSV Upload", "FOLIO", "/data_export_upload"),
    PluginApp("Data Export MARC Download", "FOLIO", "/data_export_download"),
    PluginApp("Data Export OCLC Reports", "FOLIO", "/data_export_oclc_reports"),
    PluginApp(
        "Digital Bookplates Batch Upload", "FOLIO", "/digital_bookplates_batch_upload"
    ),
    PluginApp(
        "Digital Bookplates File Download", "FOLIO", "/digital_bookplates_download"
    ),
    PluginApp("FOLIO Authority Deletes Upload", "FOLIO", "/delete_authority_records"),
    PluginApp("Google Scanning Upload", "FOLIO", "/google_scanning"),
    PluginApp("Orafin Feeder-files and Reports", "FOLIO", "/orafin"),
    PluginApp("SDR Missing Barcodes Reports", "FOLIO", "/sdr"),
    # Named "Dashboard" because it reads under a "Vendor Management" menu, which is
    # how Airflow's own nav presents it too.
    PluginApp("Dashboard", "Vendor Management", "/vendor_management"),
)


def _grouped() -> tuple[tuple[str, tuple[PluginApp, ...]], ...]:
    """
    Bucket the apps into their menus, each menu's contents sorted by name.
    """
    groups: dict[str, list[PluginApp]] = {}
    for app in APPS:
        groups.setdefault(app.category, []).append(app)
    return tuple(
        (category, tuple(sorted(apps, key=lambda app: app.name)))
        for category, apps in groups.items()
    )


NAV_GROUPS = _grouped()
"""``APPS`` bucketed by category, each menu alphabetized, so the template stays dumb."""


def current_app(root_path: str) -> PluginApp | None:
    """
    The app serving this request, or None.

    ``root_path`` is the mount prefix Airflow gave the app, which is empty when the app
    is unmounted, as it is under a bare test client. Matched by suffix rather than
    equality so that an API server mounted under a path of its own still resolves.
    """
    if not root_path:
        return None
    for app in APPS:
        if root_path.endswith(app.url_prefix):
            return app
    return None
