"""Unit tests for the shared http_session circuit breaker.

These verify that:

1. A Cloudflare interstitial (403 + text/html "Just a moment...") is treated
   as a retryable challenge, not a hard 4xx, so the bot stops failing user
   actions on the first soft challenge.
2. Repeated challenges open the circuit so callers short-circuit to None and
   serve from cache, instead of stacking up requests and worsening the
   challenge.
3. Successful responses reset the failure counter.

The tests patch the shared ``SESSION`` so no real network IO happens.
"""
from __future__ import annotations

import importlib
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def fresh_http():
    """Reload http_session so per-test config tweaks land cleanly."""
    from src.nadobro.services import http_session

    importlib.reload(http_session)
    return http_session


def _challenge_response() -> SimpleNamespace:
    return SimpleNamespace(
        status_code=403,
        headers={"content-type": "text/html; charset=UTF-8"},
        text='<!DOCTYPE html><title>Just a moment...</title>',
        url="https://gateway.mainnet.nado.xyz/query",
    )


def _ok_response() -> SimpleNamespace:
    return SimpleNamespace(
        status_code=200,
        headers={"content-type": "application/json"},
        text='{"status":"success"}',
        url="https://gateway.mainnet.nado.xyz/query",
    )


def test_cloudflare_challenge_is_retried(fresh_http, monkeypatch):
    """A first 403/html should be retried; the second 200 wins. Without this,
    a single Cloudflare interstitial would crash a user-initiated trade.
    """
    fresh_http._CF_RETRY_MAX = 2
    fresh_http._CF_RETRY_BASE_SECONDS = 0.0
    fresh_http._CF_RETRY_JITTER_SECONDS = 0.0
    calls = [_challenge_response(), _ok_response()]
    fake = MagicMock(side_effect=calls)
    monkeypatch.setattr(fresh_http.SESSION, "get", fake)
    result = fresh_http.cf_get("https://gateway.mainnet.nado.xyz/query", timeout=1.0)
    assert result is not None and result.status_code == 200
    assert fake.call_count == 2


def test_circuit_opens_after_threshold(fresh_http, monkeypatch):
    """Once the breaker opens, subsequent calls must short-circuit to None
    so we don't keep hammering Cloudflare during a known outage.
    """
    fresh_http._CF_RETRY_MAX = 0
    fresh_http._CF_BREAKER_THRESHOLD = 3
    fresh_http._CF_BREAKER_WINDOW_SECONDS = 60.0
    fresh_http._CF_BREAKER_COOLDOWN_SECONDS = 60.0
    monkeypatch.setattr(fresh_http.SESSION, "get", MagicMock(return_value=_challenge_response()))
    url = "https://archive.mainnet.nado.xyz/v2/symbols"
    for _ in range(3):
        fresh_http.cf_get(url, timeout=1.0)
    assert fresh_http.is_circuit_open(url) is True
    # Next call must short-circuit (returns None) rather than retrying.
    assert fresh_http.cf_get(url, timeout=1.0) is None


def test_token_bucket_caps_burst_rate(fresh_http):
    """Verify the per-host token bucket allows a burst up to ``burst`` then
    rejects further immediate acquisitions, so a single Fly machine can't
    stampede Cloudflare with N concurrent SDK calls.
    """
    fresh_http._HTTP_RPS_PER_HOST = 100.0  # fast refill so the test is quick
    fresh_http._HTTP_BURST_PER_HOST = 3.0
    fresh_http._HTTP_BUCKET_MAX_WAIT_SECONDS = 0.0  # fail-fast for the test
    host = "gateway.test-bucket.nado.xyz"
    # Burst of 3 succeeds back-to-back.
    assert fresh_http._acquire_token(host) is True
    assert fresh_http._acquire_token(host) is True
    assert fresh_http._acquire_token(host) is True
    # Fourth in the same instant is rejected (max_wait=0).
    assert fresh_http._acquire_token(host) is False


def test_token_bucket_refills(fresh_http):
    """After waiting at least 1/rps seconds, a new token is available."""
    fresh_http._HTTP_RPS_PER_HOST = 50.0  # 20ms per token
    fresh_http._HTTP_BURST_PER_HOST = 1.0
    fresh_http._HTTP_BUCKET_MAX_WAIT_SECONDS = 0.0
    host = "gateway.refill-test.nado.xyz"
    assert fresh_http._acquire_token(host) is True
    assert fresh_http._acquire_token(host) is False
    time.sleep(0.05)
    assert fresh_http._acquire_token(host) is True


def test_cf_request_short_circuits_when_bucket_starved(fresh_http, monkeypatch):
    """When the bucket can't yield a token within max_wait, cf_request must
    return None instead of issuing the call. Prevents queued threads from
    piling onto Cloudflare after a transient burst."""
    fresh_http._HTTP_RPS_PER_HOST = 1.0
    fresh_http._HTTP_BURST_PER_HOST = 0.0  # always empty
    fresh_http._HTTP_BUCKET_MAX_WAIT_SECONDS = 0.0
    fake = MagicMock()
    monkeypatch.setattr(fresh_http.SESSION, "get", fake)
    result = fresh_http.cf_get("https://gateway.starved.nado.xyz/query", timeout=1.0)
    assert result is None
    assert fake.call_count == 0


def test_success_resets_failure_counter(fresh_http, monkeypatch):
    fresh_http._CF_RETRY_MAX = 0
    fresh_http._CF_BREAKER_THRESHOLD = 5
    responses = [_challenge_response(), _challenge_response(), _ok_response()]
    fake = MagicMock(side_effect=responses)
    monkeypatch.setattr(fresh_http.SESSION, "get", fake)
    url = "https://gateway.testnet.nado.xyz/query"
    fresh_http.cf_get(url, timeout=1.0)  # challenge
    fresh_http.cf_get(url, timeout=1.0)  # challenge
    fresh_http.cf_get(url, timeout=1.0)  # ok
    snap = fresh_http.breaker_snapshot()
    host = "gateway.testnet.nado.xyz"
    assert snap[host]["recent_failures"] == 0
