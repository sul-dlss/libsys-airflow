import fnmatch
import re

from pathlib import Path

PLUGINS_DIR = Path(__file__).resolve().parent.parent / "libsys_airflow" / "plugins"

AIRFLOW_PLUGIN_CLASS_RE = re.compile(r"class\s+\w+\(\s*AirflowPlugin\s*\)")


def _load_airflowignore_patterns(path: Path) -> list[tuple[str, bool]]:
    patterns = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        negate = line.startswith("!")
        patterns.append((line[1:] if negate else line, negate))
    return patterns


def _is_ignored(filename: str, patterns: list[tuple[str, bool]]) -> bool:
    ignored = False
    for pattern, negate in patterns:
        if fnmatch.fnmatch(filename, pattern):
            ignored = not negate
    return ignored


def test_airflow_plugin_classes_are_not_excluded_by_airflowignore():
    """
    Airflow's plugin loader execs every .py file under plugins/ that
    .airflowignore doesn't exclude, checking each for an AirflowPlugin
    subclass. That ignore file's `*.py` / `!main.py` rule assumes every
    AirflowPlugin subclass lives in a file named main.py -- a plugin class
    defined anywhere else would be silently excluded from the scan and
    never register, with no error raised. This guards that assumption.
    """
    patterns = _load_airflowignore_patterns(PLUGINS_DIR / ".airflowignore")

    plugin_class_files = [
        path
        for path in PLUGINS_DIR.rglob("*.py")
        if AIRFLOW_PLUGIN_CLASS_RE.search(path.read_text())
    ]

    assert plugin_class_files, "Expected to find at least one AirflowPlugin subclass"

    excluded = [path for path in plugin_class_files if _is_ignored(path.name, patterns)]
    assert not excluded, (
        "These files define an AirflowPlugin subclass but are excluded by "
        f".airflowignore, so they will never be scanned/registered: {excluded}"
    )
