"""
The shared navigation partial, which every plugin app renders because Airflow links to
the apps rather than framing them and so gives them no chrome of their own.

Boundwith stands in for the apps that extend base.html, because its views need no
mocking. The per-app tests assert that the two standalone template families pick the
partial up.

Assertions are scoped to the nav element rather than the whole page: several apps
legitimately link to their own root from their own breadcrumbs, so "this app is not
linked" is only meaningful inside the nav.
"""

import re

from bs4 import BeautifulSoup

from libsys_airflow.plugins.boundwith.boundwith_view import app
from libsys_airflow.plugins.shared.nav import APPS, NAV_GROUPS, current_app

from tests.auth_helpers import authenticated_app_fixture
from tests.csrf_helpers import csrf_test_client

authenticated = authenticated_app_fixture(app)
client = csrf_test_client(app)


def _nav(response):
    nav = BeautifulSoup(response.text, "html.parser").find(class_="plugin-nav")
    assert nav is not None, "the navigation partial did not render"
    return nav


def _menus(nav):
    """Each menu as ``{category: [item text, ...]}``, in the order rendered."""
    return {
        menu.summary.get_text(strip=True): [
            item.get_text(strip=True) for item in menu.find_all("li")
        ]
        for menu in nav.find_all(class_="plugin-nav-menu")
    }


def _hrefs(nav):
    return {anchor["href"] for anchor in nav.find_all("a")}


def _without_comments(css):
    return re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)


def test_nav_is_rendered():
    response = client.get("/")

    assert response.status_code == 200
    assert _nav(response) is not None


def test_nav_links_back_to_airflow():
    assert "/" in _hrefs(_nav(client.get("/")))


def test_nav_puts_every_app_in_its_category_menu():
    menus = _menus(_nav(client.get("/")))

    assert list(menus) == ["FOLIO", "Vendor Management"]
    assert menus["Vendor Management"] == ["Dashboard"]
    assert len(menus["FOLIO"]) == 11


def test_each_menu_is_alphabetized():
    for category, items in _menus(_nav(client.get("/"))).items():
        assert items == sorted(items), category


def test_nav_links_to_every_sibling_app():
    # Unmounted, so root_path is empty and even boundwith renders as a link.
    hrefs = _hrefs(_nav(client.get("/")))

    assert {plugin_app.href for plugin_app in APPS} <= hrefs


def test_nav_hrefs_keep_their_trailing_slash():
    """A bare /orafin misses the Starlette mount and Airflow's catch-all answers it."""
    app_hrefs = _hrefs(_nav(client.get("/"))) - {"/"}

    assert app_hrefs and all(href.endswith("/") for href in app_hrefs)


def test_menus_render_the_view_names_unaltered():
    """
    The vendor app's view is named "Dashboard", which only reads sensibly under a
    "Vendor Management" menu -- the same way Airflow's own nav presents it.
    """
    menus = _menus(_nav(client.get("/")))
    rendered = {item for items in menus.values() for item in items}

    assert rendered == {plugin_app.name for plugin_app in APPS}


def test_nav_marks_the_current_app_and_does_not_link_to_it():
    """Airflow mounts each app under its url_prefix, which lands in root_path."""
    nav = _nav(csrf_test_client(app, root_path="/boundwith").get("/"))

    current = nav.find(attrs={"aria-current": "page"})
    assert current.get_text(strip=True) == "Boundwith CSV Upload"
    assert "/boundwith/" not in _hrefs(nav)
    # The rest of the nav still links out.
    assert "/orafin/" in _hrefs(nav)


def test_the_current_app_is_matched_by_its_whole_mount():
    """
    The nav assumes the API server is at the domain root, which it is: [api] base_url is
    a bare host in dev, stage and prod. Were Airflow ever mounted under a path of its
    own, resolving the current app here would not be enough -- PluginApp.href, the link
    back to Airflow in _nav.html and login_redirect.is_returnable are all root-absolute
    and would each need the prefix too. So this resolves nothing rather than rendering a
    page whose every link is wrong.
    """
    assert current_app("/boundwith").name == "Boundwith CSV Upload"
    assert current_app("/airflow/boundwith") is None
    assert current_app("") is None


def test_nav_flags_the_menu_holding_the_current_app():
    """So the user can see which menu they are in without opening all of them."""
    nav = _nav(csrf_test_client(app, root_path="/boundwith").get("/"))

    flagged = [
        menu.summary.get_text(strip=True)
        for menu in nav.find_all(class_="plugin-nav-menu")
        if menu.has_attr("data-current")
    ]
    assert flagged == ["FOLIO"]


def test_nav_is_rendered_for_pages_rendered_from_a_post():
    """
    Unlike a GET-only partial: a page re-rendered from a failed POST needs navigation as
    much as any other.
    """
    response = client.post("/create", data={"sunid": "testuser"})

    assert response.status_code == 200
    assert "Missing Boundwith Relationship File" in response.text
    assert _nav(response) is not None


def test_nav_needs_no_javascript():
    """
    The menus are <details>/<summary>, so nothing here runs script on a plugin page.
    """
    nav = _nav(client.get("/"))

    assert nav.find("script") is None
    assert len(nav.find_all("details")) == len(NAV_GROUPS)


def test_nav_styles_avoid_rem_units():
    """
    The vendor app templates load Bootstrap 3, which sets ``html { font-size: 10px }``.
    A rem unit anywhere in this partial therefore renders the nav at roughly 56% scale
    on those pages while looking correct everywhere else, so it is sized in px and em.
    """
    soup = BeautifulSoup(client.get("/").text, "html.parser")
    blocks = [
        block.get_text()
        for block in soup.find_all("style")
        if ".plugin-nav" in block.get_text()
    ]

    # Only this partial's own block. base.html's separate block does use rem, which is
    # fine because those pages do not load Bootstrap.
    assert len(blocks) == 1
    # Matched as a unit rather than as a substring, so the comment explaining all this
    # does not trip the test.
    assert re.search(r"[\d.]+\s*rem\b", _without_comments(blocks[0])) is None


def test_nothing_needs_a_right_click_workaround():
    """
    The apps are no longer framed, so links to Airflow's own pages navigate normally and
    the sandbox hints that used to apologise for that are gone.
    """
    text = client.get("/").text

    assert "right-click" not in text
    assert "tooltip-hint" not in text
