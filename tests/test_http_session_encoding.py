"""The shared HTTP session must never advertise an encoding it can't decode.

2026-07-14 incident: the session hard-coded ``Accept-Encoding: gzip, deflate,
br`` while no brotli decoder was installed. nadoexplorer's edge honored ``br``
and every leaderboard response arrived as raw brotli bytes that
``resp.json()`` could not parse ("Expecting value: line 1 column 1 (char 0)"),
so the Top Traders screen always fell back to "leaderboard unavailable". The
header is now derived from urllib3's own ACCEPT_ENCODING constant — computed
from the very imports urllib3 uses to decode — so the two cannot disagree.
"""

import importlib.util

from src.nadobro.core.http_session import SESSION, _supported_accept_encoding


def _advertised() -> list[str]:
    header = SESSION.headers.get("Accept-Encoding") or ""
    return [p.strip() for p in header.split(",") if p.strip()]


def _has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def test_session_header_matches_urllib3_decodability():
    from urllib3.util.request import ACCEPT_ENCODING

    decodable = {p.strip() for p in ACCEPT_ENCODING.split(",") if p.strip()}
    assert set(_advertised()) == decodable


def test_br_advertised_iff_a_brotli_decoder_is_importable():
    has_brotli = _has_module("brotli") or _has_module("brotlicffi")
    assert ("br" in _advertised()) == has_brotli


def test_baseline_encodings_always_advertised():
    advertised = _advertised()
    assert "gzip" in advertised and "deflate" in advertised


def test_helper_and_session_agree():
    assert SESSION.headers.get("Accept-Encoding") == _supported_accept_encoding()


def test_brotli_is_installed_in_this_environment():
    """requirements pin Brotli==1.1.0 so prod actually decodes ``br`` (keeps
    the browser-like fingerprint for the Cloudflare defense AND smaller
    payloads). If this fails, the dependency was dropped — the session would
    still be CORRECT (header shrinks automatically), but nadoexplorer traffic
    would silently lose compression and the fingerprint would change."""
    assert _has_module("brotli")
