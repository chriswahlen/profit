from __future__ import annotations

import os
import tempfile
from pathlib import Path
import unittest

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_us_stock_symbols import seed


class SeedUSStockSymbolsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_creates_entities_and_mappings(self):
        csv_path = Path(self.tmpdir.name) / "sample.csv"
        csv_path.write_text(
            """Symbol,Name,Exchange
AAPL,Apple Inc.,NASDAQ
MSFT,Microsoft Corporation,NYSE
BAD,,NASDAQ
"""
        )

        store = EntityStore(self.cfg)
        seed(csv_path, map_yfinance=True, store=store)

        conn = store.connection
        cur = conn.execute("SELECT entity_id FROM entities ORDER BY entity_id;")
        ids = [row[0] for row in cur.fetchall()]
        self.assertEqual(ids, ["sec:xnas:aapl", "sec:xnys:msft"])

        cur = conn.execute("SELECT provider, provider_entity_id, entity_id FROM provider_entity_map ORDER BY provider_entity_id;")
        mappings = cur.fetchall()
        self.assertEqual(
            mappings,
            [
                ("provider:us-stock-symbols", "AAPL", "sec:xnas:aapl"),
                ("yfinance", "AAPL", "sec:xnas:aapl"),
                ("provider:us-stock-symbols", "MSFT", "sec:xnys:msft"),
                ("yfinance", "MSFT", "sec:xnys:msft"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
