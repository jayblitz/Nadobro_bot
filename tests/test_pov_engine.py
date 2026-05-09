"""Tread Fi POV / participation engine tests (Phase 2).

Locks in:
  - compute_pov_duration multipliers match Tread spec (Aggressive 0.10,
    Normal 0.05, Passive 0.01).
  - duration_minutes scales linearly with notional and inversely with volume.
  - cycle_notional × number_of_cycles ≈ requested notional.
  - bound_user_duration_minutes clamps to [aggressive, 10× passive].
  - Defensive: zero or negative volume falls back to a finite (large) duration
    rather than crashing or returning inf/NaN.
  - normalize_preset is case-insensitive and accepts whitespace.
  - get_pair_24h_volume_usd parses the documented archive payload shape and
    caches per (network, product_id) for 60 seconds.
"""

import time
import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import pov_engine, nado_archive  # noqa: E402


class PovEngineMathTests(unittest.TestCase):
    def test_normalize_preset_canonicalizes_input(self):
        self.assertEqual(pov_engine.normalize_preset("Aggressive"), "aggressive")
        self.assertEqual(pov_engine.normalize_preset(" NORMAL "), "normal")
        self.assertEqual(pov_engine.normalize_preset("passive"), "passive")
        # Unknown / empty -> default.
        self.assertEqual(pov_engine.normalize_preset(""), pov_engine.DEFAULT_PRESET)
        self.assertEqual(pov_engine.normalize_preset("blend"), pov_engine.DEFAULT_PRESET)
        self.assertEqual(pov_engine.normalize_preset(None), pov_engine.DEFAULT_PRESET)

    def test_participation_rate_matches_tread_spec(self):
        self.assertAlmostEqual(pov_engine.participation_rate("aggressive"), 0.10, places=6)
        self.assertAlmostEqual(pov_engine.participation_rate("normal"), 0.05, places=6)
        self.assertAlmostEqual(pov_engine.participation_rate("passive"), 0.01, places=6)

    def test_duration_scales_linearly_with_notional(self):
        # Same volume + preset; doubling notional doubles duration.
        v = pov_engine.compute_pov_duration(1_000.0, "normal", 1_440_000_000.0)
        v2 = pov_engine.compute_pov_duration(2_000.0, "normal", 1_440_000_000.0)
        self.assertAlmostEqual(v2["duration_minutes"], 2.0 * v["duration_minutes"], places=6)

    def test_duration_inverse_with_volume(self):
        # Same notional + preset; halving volume doubles duration.
        v_high = pov_engine.compute_pov_duration(1_000.0, "normal", 1_440_000_000.0)
        v_low = pov_engine.compute_pov_duration(1_000.0, "normal", 720_000_000.0)
        self.assertAlmostEqual(v_low["duration_minutes"], 2.0 * v_high["duration_minutes"], places=6)

    def test_cycle_notional_recomposes_to_total(self):
        """Sum of cycle_notional × cycles ≈ requested notional."""
        notional = 5_000.0
        v = pov_engine.compute_pov_duration(notional, "normal", 1_440_000_000.0)
        cycles = max(1.0, v["duration_minutes"] / max(1e-9, v["interval_seconds"] / 60.0))
        self.assertAlmostEqual(v["cycle_notional_usd"] * cycles, notional, delta=notional * 0.001)

    def test_zero_volume_returns_large_finite_duration(self):
        # Defensive: zero volume must NOT divide by zero. Should fall back to a
        # finite (very large) duration so callers can decide whether to skip.
        v = pov_engine.compute_pov_duration(1_000.0, "normal", 0.0)
        self.assertTrue(v["duration_minutes"] > 0.0)
        self.assertNotEqual(v["duration_minutes"], float("inf"))
        self.assertEqual(v["preset"], "normal")

    def test_aggressive_completes_faster_than_passive(self):
        ag = pov_engine.compute_pov_duration(1_000.0, "aggressive", 1_440_000_000.0)
        pa = pov_engine.compute_pov_duration(1_000.0, "passive", 1_440_000_000.0)
        self.assertLess(ag["duration_minutes"], pa["duration_minutes"])

    def test_interval_decreases_with_aggression(self):
        ag = pov_engine.compute_pov_duration(1_000.0, "aggressive", 1_440_000_000.0)
        no = pov_engine.compute_pov_duration(1_000.0, "normal", 1_440_000_000.0)
        pa = pov_engine.compute_pov_duration(1_000.0, "passive", 1_440_000_000.0)
        # Per Tread plan: interval_seconds = int(60 / multiplier).
        self.assertLess(ag["interval_seconds"], no["interval_seconds"])
        self.assertLess(no["interval_seconds"], pa["interval_seconds"])


class PovBoundsTests(unittest.TestCase):
    def test_user_duration_clamped_to_aggressive_floor(self):
        clamped, lo, hi = pov_engine.bound_user_duration_minutes(
            requested_minutes=0.0001, notional_usd=1_000.0, pair_24h_volume_usd=1_440_000_000.0
        )
        self.assertGreaterEqual(clamped, lo)
        self.assertLessEqual(clamped, hi)
        self.assertAlmostEqual(clamped, lo, places=6)

    def test_user_duration_clamped_to_10x_passive_ceiling(self):
        clamped, lo, hi = pov_engine.bound_user_duration_minutes(
            requested_minutes=10**12, notional_usd=1_000.0, pair_24h_volume_usd=1_440_000_000.0
        )
        self.assertAlmostEqual(clamped, hi, places=6)

    def test_zero_request_picks_midpoint(self):
        clamped, lo, hi = pov_engine.bound_user_duration_minutes(
            requested_minutes=0.0, notional_usd=1_000.0, pair_24h_volume_usd=1_440_000_000.0
        )
        self.assertGreater(clamped, lo)
        self.assertLess(clamped, hi)

    def test_passive_ceiling_is_10x_passive_duration(self):
        passive = pov_engine.compute_pov_duration(1_000.0, "passive", 1_440_000_000.0)
        _, _, hi = pov_engine.bound_user_duration_minutes(
            requested_minutes=10**12, notional_usd=1_000.0, pair_24h_volume_usd=1_440_000_000.0
        )
        self.assertAlmostEqual(hi, 10.0 * passive["duration_minutes"], places=4)


class ArchiveVolumeFetcherTests(unittest.TestCase):
    def setUp(self) -> None:
        nado_archive._VOLUME_CACHE.clear()

    def test_volume_parsed_from_dict_cumulative_volumes(self):
        # Dict shape: cumulative_volumes keyed by int product_id, x18 scaled.
        payload = {
            "snapshots": [
                {
                    "timestamp": 1_700_000_000,
                    "cumulative_volumes": {2: int(100.0 * 1e18)},
                },
                {
                    "timestamp": 1_700_000_000 + 23 * 3600,
                    "cumulative_volumes": {2: int(1_440_100.0 * 1e18)},
                },
            ]
        }
        with patch.object(nado_archive, "_post", return_value=payload):
            vol = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=2, refresh=True)
        # Latest - oldest = 1_440_100 - 100 = 1_440_000.
        self.assertAlmostEqual(vol, 1_440_000.0, places=4)

    def test_volume_parsed_from_dict_with_string_keyed_product_id(self):
        # Some archive responses key cumulative_volumes by string product_id.
        payload = {
            "snapshots": [
                {"timestamp": 1, "cumulative_volumes": {"7": int(0)}},
                {"timestamp": 2, "cumulative_volumes": {"7": int(500.0 * 1e18)}},
            ]
        }
        with patch.object(nado_archive, "_post", return_value=payload):
            vol = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=7, refresh=True)
        self.assertAlmostEqual(vol, 500.0, places=4)

    def test_volume_parsed_from_list_shaped_cumulative_volumes(self):
        # Alternative shape: cumulative_volumes is a list of {product_id, value} rows.
        payload = {
            "snapshots": [
                {
                    "timestamp": 1,
                    "cumulative_volumes": [
                        {"product_id": 2, "cumulative_volume_x18": int(50.0 * 1e18)},
                        {"product_id": 9, "cumulative_volume_x18": int(0)},
                    ],
                },
                {
                    "timestamp": 2,
                    "cumulative_volumes": [
                        {"product_id": 2, "cumulative_volume_x18": int(150.0 * 1e18)},
                        {"product_id": 9, "cumulative_volume_x18": int(0)},
                    ],
                },
            ]
        }
        with patch.object(nado_archive, "_post", return_value=payload):
            vol = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=2, refresh=True)
        self.assertAlmostEqual(vol, 100.0, places=4)

    def test_volume_returns_none_when_archive_silent(self):
        with patch.object(nado_archive, "_post", return_value=None):
            vol = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=2, refresh=True)
        self.assertIsNone(vol)

    def test_volume_returns_none_with_only_one_snapshot(self):
        # Need at least 2 snapshots to compute a delta.
        payload = {"snapshots": [{"timestamp": 1, "cumulative_volumes": {2: int(1e18)}}]}
        with patch.object(nado_archive, "_post", return_value=payload):
            vol = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=2, refresh=True)
        self.assertIsNone(vol)

    def test_volume_cached_within_ttl(self):
        payload = {
            "snapshots": [
                {"timestamp": 1, "cumulative_volumes": {2: 0}},
                {"timestamp": 2, "cumulative_volumes": {2: int(1_000.0 * 1e18)}},
            ]
        }
        with patch.object(nado_archive, "_post", return_value=payload) as m_post:
            v1 = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=2, refresh=True)
            v2 = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=2)
        self.assertEqual(m_post.call_count, 1)
        self.assertEqual(v1, v2)

    def test_volume_normalizes_snapshot_order_by_timestamp(self):
        # Archive response in newest-first order — function must sort it.
        payload = {
            "snapshots": [
                {"timestamp": 200, "cumulative_volumes": {2: int(900.0 * 1e18)}},
                {"timestamp": 100, "cumulative_volumes": {2: int(100.0 * 1e18)}},
            ]
        }
        with patch.object(nado_archive, "_post", return_value=payload):
            vol = nado_archive.get_pair_24h_volume_usd(network="mainnet", product_id=2, refresh=True)
        self.assertAlmostEqual(vol, 800.0, places=4)


if __name__ == "__main__":
    unittest.main()
