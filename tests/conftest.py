"""Pytest: load env as soon as conftest is imported (before test modules pull in DB code)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_TESTS = _ROOT / "tests"
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
    load_dotenv(_ROOT / ".env.test", override=True)
except Exception:
    pass

# AUDIT-FIX-IS-1 test support: after the production hardening that refuses to
# use a hardcoded invite-code pepper, tests that don't already provide one
# must opt into the dev pepper (or supply ENCRYPTION_KEY themselves). Setting
# the opt-in env var here keeps the existing unit tests passing without
# weakening production safety — the dev pepper still requires this explicit
# flag, so production deployments without keys still hard-fail.
os.environ.setdefault("NADOBRO_ALLOW_DEV_INVITE_PEPPER", "true")
