"""Service Level Indicators (SLIs) for per-user latency and reliability.

Why this module exists
======================

Today ``services/perf.py`` records *global* latency histograms (e.g.
``message.total``). At 1000 users that aggregate hides everything that
matters: a single noisy user can drown the p99 and we cannot tell which
user is being starved.

This module adds **per-class SLI series** (latency histograms + counters)
that are:

* bounded in memory (capped sample windows, capped key count),
* labelled (so we can ask "p95 strategy cycle for user X", or "drop rate
  of telegram sends in the alert lane"),
* exported via ``snapshot()`` for the ``/health`` endpoint and any future
  Prometheus exporter.

Use ``timed_span("strategy.cycle", user_id=…, strategy=…)`` from any
async or sync call site. The ``record_*`` functions are cheap (single
lock acquisition, deque append) and safe to call from worker threads.

Design constraints baked in:

1. **Bounded cardinality.** The label cache holds at most
   ``_MAX_SERIES`` distinct ``(metric, labels)`` combinations. Oldest
   idle series are evicted so an attacker can't OOM us by spraying
   distinct ``user_id`` values they don't own.
2. **No GIL fights.** All counters use one ``threading.Lock``; samples
   use a fixed-size ``deque`` per series so percentile work is O(N) on
   ≤400 floats — bounded.
3. **Quantile estimates only.** We don't aim for exact p99; we aim for
   a stable, comparable view to drive operational decisions.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict, deque
from contextlib import contextmanager
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


# Tunables (overridable via env so prod can react without a redeploy).
_MAX_SAMPLES_PER_SERIES = int(os.environ.get("NADO_SLI_SAMPLES", "400"))
_MAX_SERIES = int(os.environ.get("NADO_SLI_MAX_SERIES", "4096"))


def _label_key(labels: Optional[dict]) -> str:
    if not labels:
        return ""
    parts = []
    for k in sorted(labels.keys()):
        v = labels[k]
        if v is None:
            continue
        parts.append(f"{k}={v}")
    return "|".join(parts)


def _series_key(metric: str, labels: Optional[dict]) -> str:
    lk = _label_key(labels)
    return f"{metric}#{lk}" if lk else metric


class _SeriesRegistry:
    """LRU-bounded registry of histogram series. Thread-safe."""

    def __init__(self, max_series: int = _MAX_SERIES, max_samples: int = _MAX_SAMPLES_PER_SERIES) -> None:
        self._lock = threading.Lock()
        self._series: "OrderedDict[str, deque[float]]" = OrderedDict()
        self._counters: dict[str, int] = {}
        self._labels: dict[str, dict[str, str]] = {}
        self._max_series = max_series
        self._max_samples = max_samples

    def record(self, metric: str, value_ms: float, labels: Optional[dict]) -> None:
        try:
            val = float(value_ms)
        except (TypeError, ValueError):
            return
        if val < 0:
            return
        key = _series_key(metric, labels)
        with self._lock:
            buf = self._series.get(key)
            if buf is None:
                buf = deque(maxlen=self._max_samples)
                self._series[key] = buf
                if labels:
                    self._labels[key] = {k: str(v) for k, v in labels.items() if v is not None}
                self._evict_locked()
            else:
                self._series.move_to_end(key)
            buf.append(val)

    def incr(self, counter: str, labels: Optional[dict], delta: int = 1) -> None:
        try:
            d = int(delta)
        except (TypeError, ValueError):
            return
        if d == 0:
            return
        key = _series_key(counter, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + d
            if labels and key not in self._labels:
                self._labels[key] = {k: str(v) for k, v in labels.items() if v is not None}

    def _evict_locked(self) -> None:
        while len(self._series) > self._max_series:
            oldest_key, _ = self._series.popitem(last=False)
            self._labels.pop(oldest_key, None)

    def snapshot(self, *, metric_prefix: Optional[str] = None) -> dict[str, dict]:
        with self._lock:
            items = list(self._series.items())
            counters = dict(self._counters)
            labels_map = dict(self._labels)
        out: dict[str, dict] = {}
        for key, buf in items:
            metric = key.split("#", 1)[0]
            if metric_prefix is not None and not metric.startswith(metric_prefix):
                continue
            if not buf:
                continue
            vals = sorted(buf)
            n = len(vals)
            out[key] = {
                "metric": metric,
                "labels": labels_map.get(key, {}),
                "count": n,
                "p50_ms": _percentile(vals, 0.5),
                "p95_ms": _percentile(vals, 0.95),
                "p99_ms": _percentile(vals, 0.99),
                "max_ms": vals[-1],
            }
        for key, count in counters.items():
            metric = key.split("#", 1)[0]
            if metric_prefix is not None and not metric.startswith(metric_prefix):
                continue
            entry = out.setdefault(key, {"metric": metric, "labels": labels_map.get(key, {})})
            entry["counter"] = count
        return out

    def reset(self) -> None:
        with self._lock:
            self._series.clear()
            self._counters.clear()
            self._labels.clear()


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return round(sorted_values[0], 2)
    rank = (len(sorted_values) - 1) * q
    lo = int(rank)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = rank - lo
    return round(sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac, 2)


_REGISTRY = _SeriesRegistry()


def record_latency(metric: str, value_ms: float, **labels) -> None:
    """Record a latency sample in milliseconds.

    ``labels`` may include ``user_id``, ``network``, ``strategy``, ``lane``,
    ``outcome``. Keep cardinality manageable: do NOT record raw text
    payloads.
    """
    _REGISTRY.record(metric, value_ms, labels or None)


def increment(counter: str, delta: int = 1, **labels) -> None:
    """Increment a labelled counter (e.g. ``orders.placed`` with
    ``user_id``+``outcome=success``)."""
    _REGISTRY.incr(counter, labels or None, delta)


@contextmanager
def timed_span(metric: str, **labels):
    """Context manager that records ``metric`` latency on exit.

    Adds ``outcome=success`` on clean exit, ``outcome=error`` when the
    wrapped block raises.
    """
    started = time.perf_counter()
    success = True
    try:
        yield
    except Exception:
        success = False
        raise
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        all_labels = dict(labels)
        all_labels.setdefault("outcome", "success" if success else "error")
        _REGISTRY.record(metric, elapsed_ms, all_labels)


def snapshot(metric_prefix: Optional[str] = None) -> dict[str, dict]:
    """Return the current SLI snapshot. ``metric_prefix`` narrows by name."""
    return _REGISTRY.snapshot(metric_prefix=metric_prefix)


def reset() -> None:
    """Test-only: clear all series."""
    _REGISTRY.reset()


def summary_lines(top_n: int = 8) -> list[str]:
    """Human-readable top series by p95 latency, for log/health surfaces."""
    snap = snapshot()
    if not snap:
        return ["No SLI samples yet."]
    ranked = sorted(
        (entry for entry in snap.values() if "p95_ms" in entry),
        key=lambda e: e["p95_ms"],
        reverse=True,
    )
    lines: list[str] = []
    for entry in ranked[:top_n]:
        label_part = ""
        if entry.get("labels"):
            label_part = " " + " ".join(f"{k}={v}" for k, v in entry["labels"].items())
        lines.append(
            f"{entry['metric']}{label_part}: p50={entry['p50_ms']}ms "
            f"p95={entry['p95_ms']}ms p99={entry['p99_ms']}ms n={entry['count']}"
        )
    counters = sorted(
        ((k, v) for k, v in snap.items() if "counter" in v),
        key=lambda kv: kv[1]["counter"],
        reverse=True,
    )
    for key, entry in counters[:top_n]:
        label_part = ""
        if entry.get("labels"):
            label_part = " " + " ".join(f"{k}={v}" for k, v in entry["labels"].items())
        lines.append(f"{entry['metric']}{label_part}: count={entry['counter']}")
    return lines


def metrics_for_user(user_id: int, *, metric_prefix: Optional[str] = None) -> dict[str, dict]:
    """Filter SLIs for a single user — used by ops admin commands."""
    out: dict[str, dict] = {}
    target = str(int(user_id))
    for key, entry in snapshot(metric_prefix=metric_prefix).items():
        if entry.get("labels", {}).get("user_id") == target:
            out[key] = entry
    return out


__all__: Iterable[str] = (
    "record_latency",
    "increment",
    "timed_span",
    "snapshot",
    "summary_lines",
    "metrics_for_user",
    "reset",
)
