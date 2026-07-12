"""Static guard: env vars must be read via utils/env.py helpers, not raw casts.

Deploy dashboards / .env templates in this project carry inline ``# comments``.
A raw ``int(os.environ.get(...))`` at module level turns a commented value into
a boot-time ValueError (and a manual truthy-parse silently reads False). This
sweep keeps the comment-tolerant helpers (env_int/env_float/env_bool/env_str,
clean_env_value) the only way env values get parsed.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCAN_ROOTS = [REPO / "src" / "nadobro", REPO / "main.py"]
EXEMPT = {REPO / "src" / "nadobro" / "utils" / "env.py"}

# int(/float(/Decimal( wrapped directly around an os.environ / os.getenv read.
RAW_NUMERIC_CAST = re.compile(
    r"\b(?:int|float|Decimal)\(\s*os\.(?:environ\.get|environ\[|getenv)\b"
)
# Manual truthy parse of a raw env read (the pre-env_bool idiom).
MANUAL_TRUTHY = re.compile(
    r"os\.(?:environ\.get|getenv)\([^)]*\)\s*(?:\.strip\(\))?\s*(?:\.lower\(\))?\s*in\s*\(\s*['\"]1['\"]"
)


def _iter_files():
    for root in SCAN_ROOTS:
        if root.is_file():
            yield root
        else:
            yield from root.rglob("*.py")


def test_no_raw_numeric_env_casts():
    offenders = []
    for path in _iter_files():
        if path in EXEMPT:
            continue
        text = path.read_text()
        for m in RAW_NUMERIC_CAST.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            offenders.append(f"{path.relative_to(REPO)}:{line}")
    assert not offenders, (
        "raw numeric cast of an env value (breaks on inline '# comment'); "
        "use env_int/env_float from src/nadobro/utils/env.py:\n  "
        + "\n  ".join(offenders)
    )


def test_no_manual_truthy_env_parses():
    offenders = []
    for path in _iter_files():
        if path in EXEMPT:
            continue
        text = path.read_text()
        for m in MANUAL_TRUTHY.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            offenders.append(f"{path.relative_to(REPO)}:{line}")
    assert not offenders, (
        "manual truthy parse of an env value; use env_bool from "
        "src/nadobro/utils/env.py:\n  " + "\n  ".join(offenders)
    )
