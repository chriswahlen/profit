from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
import unittest

from config import Config
from scripts.seed_currencies import seed_currencies


class SeedCurrenciesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_path = Path(self.tmpdir.name) / "data"
        os.environ["PROFIT_DATA_PATH"] = str(self.data_path)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def _connect_entities_db(self) -> sqlite3.Connection:
        cfg = Config()
        db_path = Path(cfg.data_path()) / "entities.sqlite"
        return sqlite3.connect(db_path)

    def test_seeds_and_maps_currencies(self) -> None:
        cfg = Config()
        currency_map = {
            "USD": "United States Dollar",
            "EUR": "Euro",
        }

        seed_currencies(config=cfg, currency_map=currency_map)

        with self._connect_entities_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT entity_id, name FROM entities ORDER BY entity_id;")
            rows = cur.fetchall()
            self.assertEqual(rows, [("ccy:eur", "Euro"), ("ccy:usd", "United States Dollar")])

            cur.execute("SELECT provider, provider_entity_id, entity_id FROM provider_entity_map ORDER BY provider_entity_id;")
            provider_rows = cur.fetchall()
            self.assertEqual(
                provider_rows,
                [("openexchangerates", "EUR", "ccy:eur"), ("openexchangerates", "USD", "ccy:usd")],
            )

            cur.execute("SELECT provider FROM providers;")
            self.assertEqual(cur.fetchone()[0], "openexchangerates")

    def test_idempotent_upsert(self) -> None:
        cfg = Config()
        currency_map = {"USD": "United States Dollar"}

        seed_currencies(config=cfg, currency_map=currency_map)
        seed_currencies(config=cfg, currency_map=currency_map)

        with self._connect_entities_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM entities;")
            self.assertEqual(cur.fetchone()[0], 1)

            cur.execute("SELECT COUNT(*) FROM provider_entity_map;")
            self.assertEqual(cur.fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
