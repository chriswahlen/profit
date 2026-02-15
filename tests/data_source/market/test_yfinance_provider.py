from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
import types
import unittest

from config import Config
from data_sources.entity import EntityStore
from data_sources.market.yfinance_provider import YFinanceProviderAdapter


class FakeRow(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class FakeHistory:
    def __init__(self):
        self._rows = [
            (datetime(2024, 2, 1), FakeRow({"Open": 10, "High": 11, "Low": 9, "Close": 10.5, "Adj Close": 10.4, "Volume": 1000})),
            (datetime(2024, 2, 2), FakeRow({"Open": 10.5, "High": 11.5, "Low": 10, "Close": 11, "Adj Close": 10.9, "Volume": 1200, "Dividends": 0.1})),
        ]

    def iterrows(self):
        return iter(self._rows)


class FakeTicker:
    def __init__(self, symbol):
        self.symbol = symbol

    def history(self, **kwargs):  # pragma: no cover - simple passthrough
        return FakeHistory()


class FakeYFModule(types.SimpleNamespace):
    def __init__(self):
        super().__init__(Ticker=FakeTicker)


class YFinanceProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.entity_store = EntityStore(self.cfg)
        # Inject fake yfinance module before adapter import time lookup
        sys.modules["yfinance"] = FakeYFModule()

    def tearDown(self) -> None:
        sys.modules.pop("yfinance", None)
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_fetch_outputs_candles(self):
        provider = YFinanceProviderAdapter(config=self.cfg, entity_store=self.entity_store)
        candles = list(provider.fetch(["sec:xnas:aapl"]))

        self.assertEqual(len(candles), 2)
        c1 = candles[0]
        self.assertEqual(c1.canonical_id, "sec:xnas:aapl")
        self.assertEqual(c1.start_ts, "2024-02-01")
        self.assertEqual(c1.adj_close, 10.4)
        self.assertIsNone(c1.dividend)
        self.assertEqual(candles[1].dividend, 0.1)


if __name__ == "__main__":
    unittest.main()
