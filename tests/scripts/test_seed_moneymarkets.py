from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import json

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_moneymarkets import rows_from_csv, seed_rows


class MoneyMarketSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_rows_creates_single_fund_and_relations(self) -> None:
        csv_path = Path(self.tmpdir.name) / "money.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,family,exchange\n"
            "SPXMM,Sample Money Market,USD,Short-term income,Some Family,NYS\n"
            "SPXMM,Sample Money Market Duplicate,USD,Short-term income,Some Family,NYS\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)

        self.assertEqual(inserted, 1)
        self.assertEqual(skipped, 0)

        fund_rows = self.store.connection.execute(
            "SELECT entity_id, metadata FROM entities WHERE entity_type='fund';"
        ).fetchall()
        self.assertEqual(len(fund_rows), 1)
        fund_id, metadata = fund_rows[0]
        self.assertEqual(fund_id, "fund:xnys:spxmm")
        payload = json.loads(metadata)
        self.assertEqual(payload["currency"], "ccy:usd")
        self.assertEqual(payload["family"], "Some Family")
        self.assertEqual(payload["summary"], "Short-term income")

        provider_rows = self.store.connection.execute(
            "SELECT provider_entity_id FROM provider_entity_map WHERE provider='provider:financedatabase';"
        ).fetchall()
        self.assertEqual([row[0] for row in provider_rows], ["SPXMM"])

        listed_relations = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id FROM entity_entity_map WHERE relation='listed_on';"
        ).fetchall()
        self.assertEqual(len(listed_relations), 1)
        self.assertEqual(listed_relations[0][0], fund_id)
        self.assertEqual(listed_relations[0][1], "mic:xnys")

    def test_skips_missing_symbol(self) -> None:
        rows = [{"symbol": "", "name": "", "currency": "", "summary": "", "family": "", "exchange": ""}]
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 0)
        self.assertEqual(skipped, 1)
