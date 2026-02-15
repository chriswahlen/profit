from __future__ import annotations

import os
import tempfile
from pathlib import Path
import unittest

from config import Config
from data_sources.entity import EntityStore, EntityType
from scripts.seed_ticker_list import seed, load_tickers


class SeedTickerListTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_security_tickers(self):
        ticker_path = Path(self.tmpdir.name) / "tickers.txt"
        ticker_path.write_text("AAPL\nMSFT\n \n")

        store = EntityStore(self.cfg)
        seed(
            mic="XNAS",
            entity_type=EntityType.SECURITY,
            tickers=["AAPL", "MSFT", ""],
            provider="provider:test",
            store=store,
        )

        cur = store.connection.execute(
            "SELECT entity_id, name FROM entities ORDER BY entity_id;"
        )
        rows = cur.fetchall()
        self.assertEqual(rows, [("sec:xnas:aapl", "AAPL"), ("sec:xnas:msft", "MSFT")])
        cur = store.connection.execute("SELECT count(*) FROM provider_entity_map;")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_seed_unsupported_entity_type(self):
        store = EntityStore(self.cfg)
        with self.assertRaises(ValueError):
            seed(
                mic="XNAS",
                entity_type=EntityType.COMPANY,
                tickers=["AAPL"],
                provider="provider:test",
                store=store,
            )

    def test_loads_from_file_url(self):
        ticker_file = Path(self.tmpdir.name) / "url_tickers.txt"
        ticker_file.write_text("AAPL\nMSFT\n")
        source = ticker_file.as_uri()

        tickers = load_tickers(source)
        self.assertEqual(tickers, ["AAPL", "MSFT"])


if __name__ == "__main__":
    unittest.main()
