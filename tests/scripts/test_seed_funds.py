from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import json

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_funds import rows_from_csv, seed_rows


class FundSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_rows_creates_fund_and_relations(self) -> None:
        csv_path = Path(self.tmpdir.name) / "funds.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,family,exchange\n"
            "GOLD,Synthetic Gold Shield,USD,Gives pure exposure to gold,Commodities,ETP,Gold Managers,NYS\n"
            "GOLD,Synthetic Gold Shield Duplicate,USD,Gives pure exposure to gold,Commodities,ETP,Gold Managers,NYS\n"
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
        self.assertEqual(fund_id, "fund:gold")
        payload = json.loads(metadata)
        self.assertEqual(payload["currency"], "ccy:usd")
        self.assertEqual(payload["summary"], "Gives pure exposure to gold")
        self.assertEqual(payload["category_group"], "Commodities")
        self.assertEqual(payload["category"], "ETP")
        self.assertEqual(payload["family"], "Gold Managers")

        provider_rows = self.store.connection.execute(
            "SELECT provider_entity_id FROM provider_entity_map WHERE provider='provider:financedatabase';"
        ).fetchall()
        self.assertEqual([row[0] for row in provider_rows], ["GOLD"])

        listed_relations = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id FROM entity_entity_map WHERE relation='listed_on';"
        ).fetchall()
        self.assertEqual(len(listed_relations), 1)
        self.assertEqual(listed_relations[0][0], fund_id)
        self.assertEqual(listed_relations[0][1], "mic:xnys")

        family_relations = self.store.connection.execute(
            "SELECT dst_entity_id FROM entity_entity_map WHERE relation='managed_by';"
        ).fetchall()
        self.assertEqual(len(family_relations), 1)
        self.assertEqual(family_relations[0][0], "company:us:gold-managers")

    def test_skips_missing_symbol(self) -> None:
        rows = [
            {
                "symbol": "",
                "name": "",
                "currency": "",
                "summary": "",
                "category_group": "",
                "category": "",
                "family": "",
                "exchange": "",
            }
        ]
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 0)
        self.assertEqual(skipped, 1)
