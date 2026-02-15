from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import unittest

from config import Config
from data_sources.entity import EntityStore
from data_sources.market.market_data_source import MarketDataSource
from data_sources.market.market_data_store import Candle


class FakeProvider:
    def __init__(self, name: str, candles):
        self.name = name
        self._candles = candles

    def fetch(self, entity_ids):  # pragma: no cover - trivial passthrough
        return list(self._candles)


class MarketDataSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.entity_store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def _connect_db(self):
        db_path = Path(self.cfg.data_path()) / "market_ohlcv.sqlite"
        return sqlite3.connect(db_path)

    def test_best_view_prefers_priority(self):
        candle_time = "2026-02-10 00:00:00"
        c_alpha = Candle(
            canonical_id="XNYS:AAPL",
            instrument_type="security",
            interval="1d",
            start_ts=candle_time,
            open=10,
            high=11,
            low=9,
            close=10.5,
            volume=1_000,
            provider="alpha",
        )
        c_beta = Candle(
            canonical_id="XNYS:AAPL",
            instrument_type="security",
            interval="1d",
            start_ts=candle_time,
            open=9,
            high=12,
            low=8,
            close=11,
            volume=1_200,
            provider="beta",
        )
        providers = [FakeProvider("alpha", [c_alpha]), FakeProvider("beta", [c_beta])]
        ds = MarketDataSource(self.cfg, self.entity_store, providers)

        ds.store.upsert_provider_priority(provider="alpha", priority=1)
        ds.store.upsert_provider_priority(provider="beta", priority=5)

        res = ds.ensure_up_to_date(["XNYS:AAPL"])
        self.assertEqual(res.failed, 0)

        with self._connect_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM candles_raw;")
            self.assertEqual(cur.fetchone()[0], 2)

            cur.execute("SELECT provider, open, close FROM candles_best;")
            provider, open, close = cur.fetchone()
            self.assertEqual(provider, "alpha")
            self.assertEqual(open, 10)
            self.assertEqual(close, 10.5)

    def test_instrument_override_beats_global_priority(self):
        candle_time = "2026-02-12 00:00:00"
        alpha = Candle(
            canonical_id="fx:usd:eur",
            instrument_type="forex",
            interval="1d",
            start_ts=candle_time,
            close=1.08,
            provider="alpha",
        )
        beta = Candle(
            canonical_id="fx:usd:eur",
            instrument_type="forex",
            interval="1d",
            start_ts=candle_time,
            close=1.07,
            provider="beta",
        )
        providers = [FakeProvider("alpha", [alpha]), FakeProvider("beta", [beta])]
        ds = MarketDataSource(self.cfg, self.entity_store, providers)

        # Globally alpha wins.
        ds.store.upsert_provider_priority(provider="alpha", priority=1)
        ds.store.upsert_provider_priority(provider="beta", priority=2)
        # Override for this instrument to prefer beta.
        ds.store.upsert_instrument_provider_rank(canonical_id="fx:usd:eur", provider="beta", priority=0)

        res = ds.ensure_up_to_date(["fx:usd:eur"])
        self.assertEqual(res.failed, 0)

        with self._connect_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT provider, close FROM candles_best WHERE canonical_id=?", ("fx:usd:eur",))
            provider, close = cur.fetchone()
            self.assertEqual(provider, "beta")
            self.assertEqual(close, 1.07)


if __name__ == "__main__":
    unittest.main()
