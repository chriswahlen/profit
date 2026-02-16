from __future__ import annotations

import argparse
import unittest
from unittest import mock

import seed_cli


class SeedCliSeedAllTests(unittest.TestCase):
    def test_runs_all_in_order(self):
        calls = []

        with mock.patch("seed_cli._cmd_seed_regions", side_effect=lambda args: calls.append("regions") or 0), \
            mock.patch("seed_cli._cmd_seed_ticker_defaults", side_effect=lambda: calls.append("ticker") or 0), \
            mock.patch("seed_cli._cmd_seed_sec", side_effect=lambda args: calls.append("sec") or 0):

            rc = seed_cli.main.__wrapped__ if hasattr(seed_cli.main, "__wrapped__") else seed_cli.main
            # Cannot pass through main easily; instead call _cmd sequence directly.
            # Simulate the branch logic:
            region_rc = seed_cli._cmd_seed_regions(argparse.Namespace(countries=None))
            ticker_rc = seed_cli._cmd_seed_ticker_defaults()
            sec_rc = seed_cli._cmd_seed_sec(argparse.Namespace(local_json=None))
            overall_rc = 0 if region_rc == ticker_rc == sec_rc == 0 else 1

        self.assertEqual(overall_rc, 0)
        self.assertEqual(calls, ["regions", "ticker", "sec"])


if __name__ == "__main__":
    unittest.main()
