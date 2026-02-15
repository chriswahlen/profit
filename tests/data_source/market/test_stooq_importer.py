from __future__ import annotations

import os
import sqlite3
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
import unittest

from config import Config
from data_sources.market.stooq_importer import StooqImporter
from data_sources.market.market_data_store import MarketDataStore
from data_sources.entity import EntityStore


def make_sample_zip(base: Path) -> Path:
    path = base / "sample_stooq.zip"
    data = """<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
AAPL.US,D,20240201,000000,100,110,95,105,1000,0
AAPL.US,D,20240202,000000,105,115,100,110,1500,0
"""
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("data/daily/us/nasdaq/aapl.us.txt", data)
    return path


class StooqImporterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmpdir.name) / "data"
        os.environ["PROFIT_DATA_PATH"] = str(self.data_dir)
        self.cfg = Config()

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def _connect_market_db(self):
        return sqlite3.connect(Path(self.cfg.data_path()) / "market_ohlcv.sqlite")

    def test_imports_candles_and_entities(self):
        zip_path = make_sample_zip(Path(self.tmpdir.name))
        importer = StooqImporter(config=self.cfg, zip_paths=[zip_path])
        importer.import_all()

        with self._connect_market_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM candles_raw;")
            self.assertEqual(cur.fetchone()[0], 2)
            cur.execute("SELECT provider, close FROM candles_best ORDER BY start_ts;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], "stooq")
            self.assertEqual(rows[0][1], 105)

        entity_db = sqlite3.connect(Path(self.cfg.data_path()) / "entities.sqlite")
        cur = entity_db.cursor()
        cur.execute("SELECT entity_id, entity_type FROM entities;")
        eid, etype = cur.fetchone()
        self.assertEqual(eid, "sec:xnas:aapl")
        self.assertEqual(etype, "security")
        entity_db.close()


if __name__ == "__main__":
    unittest.main()
