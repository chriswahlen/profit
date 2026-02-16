from __future__ import annotations

import argparse
import unittest
from unittest import mock

import seed_cli
import scripts.seed_exchanges as seed_exchanges
from config import Config


class SeedCliSeedAllTests(unittest.TestCase):
    def test_runs_all_in_order(self):
        calls = []

        with mock.patch("scripts.seed_currencies.seed_currencies", side_effect=lambda cfg: calls.append("currencies") or 0), \
            mock.patch("seed_cli._cmd_seed_regions", side_effect=lambda ns: calls.append("regions") or 0), \
            mock.patch("scripts.seed_exchanges.main", side_effect=lambda args=None: calls.append("exchanges") or 0), \
            mock.patch("seed_cli._cmd_seed_sec", side_effect=lambda ns: calls.append("sec") or 0):

            from scripts.seed_currencies import seed_currencies
            currency_rc = seed_currencies(Config())
            region_rc = seed_cli._cmd_seed_regions(argparse.Namespace(countries=None))
            exch_rc = seed_exchanges.main([])
            sec_rc = seed_cli._cmd_seed_sec(argparse.Namespace(local_json=None))

        self.assertEqual(calls, ["currencies", "regions", "exchanges", "sec"])
        self.assertEqual(currency_rc, 0)
        self.assertEqual(region_rc, 0)
        self.assertEqual(exch_rc, 0)
        self.assertEqual(sec_rc, 0)


if __name__ == "__main__":
    unittest.main()
