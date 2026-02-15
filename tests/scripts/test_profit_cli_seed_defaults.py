from __future__ import annotations

import unittest
from unittest import mock

from profit_cli import _cmd_seed_ticker_defaults, NASDAQ_TICKER_URL, NYSE_TICKER_URL, AMEX_TICKER_URL
from data_sources.entity import EntityType


class ProfitCliSeedDefaultsTests(unittest.TestCase):
    def test_uses_hardcoded_nasdaq_url_and_mic(self):
        dummy_store = object()

        class DummyManager:
            def __init__(self, config=None):
                self.entity_store = dummy_store

        with mock.patch("profit_cli.DataSourceManager", DummyManager), \
            mock.patch("profit_cli.load_tickers", side_effect=[["ABC", "XYZ"], ["IBM"], ["GSAT"]]) as load_mock, \
            mock.patch("profit_cli.seed") as seed_mock:

            rc = _cmd_seed_ticker_defaults()

        self.assertEqual(rc, 0)
        load_mock.assert_any_call(NASDAQ_TICKER_URL)
        load_mock.assert_any_call(NYSE_TICKER_URL)
        load_mock.assert_any_call(AMEX_TICKER_URL)
        seed_mock.assert_any_call(
            mic="XNAS",
            entity_type=EntityType.SECURITY,
            tickers=["ABC", "XYZ"],
            provider="provider:nasdaq-tickers",
            store=dummy_store,
        )
        seed_mock.assert_any_call(
            mic="XNYS",
            entity_type=EntityType.SECURITY,
            tickers=["IBM"],
            provider="provider:nyse-tickers",
            store=dummy_store,
        )
        seed_mock.assert_any_call(
            mic="XASE",
            entity_type=EntityType.SECURITY,
            tickers=["GSAT"],
            provider="provider:amex-tickers",
            store=dummy_store,
        )


if __name__ == "__main__":
    unittest.main()
