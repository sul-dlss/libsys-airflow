import pathlib
import re


def invoice_count(edit_path: pathlib.Path) -> int:
    with edit_path.open("r", errors="ignore") as fo:
        text = fo.read()
        return len(re.findall(r"BGM\+380", text))
