"""NADO_DESK_RESUME_ON_RESTART must be fail-closed.

The redeploy contract is: boot = stand-down, nothing trades without the user
starting it (CLAUDE.md hard rule). This flag is the legacy escape hatch, so it
must only read True for a bare, unambiguous truthy token. In particular an
inline-commented value ("true # note") — which the comment-tolerant env_bool
would honor — must stay False here: such values were inert historically, and a
parser upgrade must never widen the auto-resume hatch on an already-deployed
environment.
"""

import pytest

from src.nadobro.trading.desk_runtime import desk_resume_on_restart


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("NADO_DESK_RESUME_ON_RESTART", raising=False)
    yield


def test_default_is_off():
    assert desk_resume_on_restart() is False


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "yes", "on", "  on  "])
def test_bare_truthy_token_enables(monkeypatch, raw):
    monkeypatch.setenv("NADO_DESK_RESUME_ON_RESTART", raw)
    assert desk_resume_on_restart() is True


@pytest.mark.parametrize(
    "raw",
    [
        "true # enable only for migration",
        "1 # temporary",
        "yes  # note",
        "false",
        "0",
        "banana",
        "",
        "   ",
    ],
)
def test_anything_else_stays_off(monkeypatch, raw):
    monkeypatch.setenv("NADO_DESK_RESUME_ON_RESTART", raw)
    assert desk_resume_on_restart() is False
