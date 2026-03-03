import logging
import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager

logger = logging.getLogger(__name__)

_MAX_SAMPLES = 400
_metrics: dict[str, deque] = defaultdict(lambda: deque(maxlen=_MAX_SAMPLES))
_lock = threading.Lock()


def record_metric(metric: str, value_ms: float) -> None:
    try:
        val = float(value_ms)
    except (TypeError, ValueError):
        return
    if val < 0:
        return
    with _lock:
        _metrics[metric].append(val)


@contextmanager
def timed_metric(metric: str):
    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        record_metric(metric, elapsed_ms)


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * p
    lo = int(rank)
    hi = min(lo + 1, len(values) - 1)
    frac = rank - lo
    return values[lo] * (1 - frac) + values[hi] * frac


def snapshot() -> dict[str, dict]:
    out = {}
    with _lock:
        items = list(_metrics.items())
    for metric, samples in items:
        vals = sorted(list(samples))
        if not vals:
            continue
        out[metric] = {
            "count": len(vals),
            "p50_ms": round(_percentile(vals, 0.50), 2),
            "p95_ms": round(_percentile(vals, 0.95), 2),
            "max_ms": round(vals[-1], 2),
            "avg_ms": round(sum(vals) / len(vals), 2),
        }
    return out


def summary_lines(top_n: int = 8) -> list[str]:
    snap = snapshot()
    if not snap:
        return ["No performance samples yet."]
    ranked = sorted(snap.items(), key=lambda kv: kv[1]["p95_ms"], reverse=True)
    lines = []
    for metric, data in ranked[:top_n]:
        lines.append(
            f"{metric}: p50={data['p50_ms']}ms p95={data['p95_ms']}ms "
            f"avg={data['avg_ms']}ms n={data['count']}"
        )
    return lines


def log_slow(metric: str, threshold_ms: float, started_at: float) -> None:
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    record_metric(metric, elapsed_ms)
    if elapsed_ms >= threshold_ms:
        logger.warning("%s slow-path %.2fms", metric, elapsed_ms)
