from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import json

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_etfs import seed_rows, rows_from_csv


class ETFSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_rows_creates_etf_and_linked_family(self) -> None:
        csv_path = Path(self.tmpdir.name) / "etfs.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,family,exchange\n"
            "SPY,SPDR S&P 500 ETF Trust,USD,Tracks the S&P 500 index.,Equities,Large Cap,State Street Global Advisors,NMS\n"
            "VTI,Vanguard Total Stock Market ETF,USD,Tracks the CRSP US Total Market Index.,Equities,Large Cap,Vanguard Asset Management,NMS\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)

        self.assertEqual(inserted, 2)
        self.assertEqual(skipped, 0)

        fund_rows = self.store.connection.execute(
            "SELECT entity_id, entity_type, name, metadata FROM entities WHERE entity_type='etf' ORDER BY entity_id;"
        ).fetchall()
        self.assertEqual(len(fund_rows), 2)
        self.assertTrue("spdr-s-p-500-etf-trust" in fund_rows[0][0])
        self.assertTrue("vanguard-total-stock-market-etf" in fund_rows[1][0])
        fund_meta = json.loads(fund_rows[0][3])
        self.assertEqual(fund_meta["category_group"], "Equities")
        self.assertEqual(fund_meta["category"], "Large Cap")
        self.assertEqual(fund_meta["summary"], "Tracks the S&P 500 index.")

        family_rows = self.store.connection.execute(
            "SELECT entity_id, entity_type, name FROM entities WHERE entity_id LIKE 'company:%' ORDER BY entity_id;"
        ).fetchall()
        self.assertEqual(len(family_rows), 2)
        self.assertTrue(any("state-street" in row[0] for row in family_rows))
        self.assertTrue(any("vanguard" in row[0] for row in family_rows))

        relations = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id, relation FROM entity_entity_map WHERE relation='managed_by' ORDER BY src_entity_id;"
        ).fetchall()
        self.assertEqual(len(relations), 2)
        self.assertEqual(relations[0][2], "managed_by")

        listed_rows = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id FROM entity_entity_map WHERE relation='listed_on' ORDER BY src_entity_id;"
        ).fetchall()
        self.assertEqual(len(listed_rows), 2)
        self.assertTrue(any(row[0].startswith("etf:") and row[1] == "mic:xnas" for row in listed_rows))

        provider_count = self.store.connection.execute(
            "SELECT COUNT(*) FROM provider_entity_map WHERE provider='provider:financedatabase';"
        ).fetchone()[0]
        self.assertEqual(provider_count, 0)

    def test_skips_rows_missing_symbol(self) -> None:
        rows = [{"symbol": "", "name": "", "currency": "", "summary": "", "category_group": "", "category": "", "family": ""}]
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 0)
        self.assertEqual(skipped, 1)
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM entities WHERE entity_type='etf';").fetchone()[0],
            0,
        )

    def test_same_product_on_multiple_exchanges_shares_entity(self) -> None:
        csv_path = Path(self.tmpdir.name) / "multi.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,family,exchange\n"
            "BE3C.HM,Berenberg Europe Focus R,EUR,Focuses on Europe,Equities,Developed Markets,,HAM\n"
            "BE3C.MU,Berenberg Europe Focus R,EUR,Focuses on Europe,Equities,Developed Markets,,MUN\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)

        self.assertEqual(inserted, 2)
        self.assertEqual(skipped, 0)

        fund_rows = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_type='etf';"
        ).fetchall()
        self.assertEqual(fund_rows, [("etf:be3c",)])

        listed = self.store.connection.execute(
            "SELECT dst_entity_id, metadata FROM entity_entity_map WHERE relation='listed_on' ORDER BY dst_entity_id;"
        ).fetchall()
        self.assertEqual(listed[0][0], "mic:xham")
        self.assertEqual(listed[1][0], "mic:xmun")
        self.assertEqual(json.loads(listed[0][1]), {"symbol": "BE3C.HM"})
        self.assertEqual(json.loads(listed[1][1]), {"symbol": "BE3C.MU"})


if __name__ == "__main__":
    unittest.main()
