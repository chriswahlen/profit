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
        entity_ids = {row[0] for row in fund_rows}
        self.assertEqual(entity_ids, {"etf:spy", "etf:vti"})
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

    def test_symbol_driven_slug(self) -> None:
        csv_path = Path(self.tmpdir.name) / "symbol.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,family,exchange\n"
            "VUCP,Vanguard USD Corporate Bond UCITS ETF,USD,Corporate bond exposure,Fixed Income,Bonds,,LON\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)

        self.assertEqual(inserted, 1)
        self.assertEqual(skipped, 0)
        entity = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_type='etf';"
        ).fetchone()[0]
        self.assertEqual(entity, "etf:vucp")

    def test_duplicate_rows_merge_metadata(self) -> None:
        csv_path = Path(self.tmpdir.name) / "merge.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,family,exchange\n"
            "BE3C.HM,Berenberg Europe Focus R,EUR,Focus on Europe,Equities,Developed Markets,,HAM\n"
            "BE3C.MU,Berenberg Europe Focus R,EUR,,Alternatives,,AltShares,MUN\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)
        self.assertEqual(inserted, 2)
        self.assertEqual(skipped, 0)

        base_meta = json.loads(
            self.store.connection.execute(
                "SELECT metadata FROM entities WHERE entity_id='etf:be3c';"
            ).fetchone()[0]
        )
        alt_meta = json.loads(
            self.store.connection.execute(
                "SELECT metadata FROM entities WHERE entity_id='etf:be3c.mu';"
            ).fetchone()[0]
        )
        self.assertEqual(
            base_meta,
            {"summary": "Focus on Europe", "category_group": "Equities", "category": "Developed Markets"},
        )
        self.assertEqual(
            alt_meta,
            {"category_group": "Alternatives", "family": "AltShares"},
        )

    def test_metadata_diff_creates_new_entity(self) -> None:
        csv_path = Path(self.tmpdir.name) / "metadata.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,family,exchange\n"
            "ARB,AltShares Merger Arbitrage ETF,USD,Merger arbitrage,Alternatives,,AltShares,PCX\n"
            "ARB.TO,Accelerate Arbitrage Fund,CAD,Accelerate arbitrage,Alternatives,,Accelerate Financial Technologies,TOR\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)
        self.assertEqual(inserted, 2)
        entity_ids = {
            row[0]
            for row in self.store.connection.execute("SELECT entity_id FROM entities WHERE entity_type='etf';").fetchall()
        }
        self.assertEqual(entity_ids, {"etf:arb", "etf:arb.to"})

    def test_multi_exchange_rows_with_same_metadata_do_not_split(self) -> None:
        csv_path = Path(self.tmpdir.name) / "same-meta.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,family,exchange\n"
            "09AA.BE,Value Investm Klas Fds T,EUR,Value strategy,,Value Fund,Value Investm,BER\n"
            "09AA.DU,Value Investm Klas Fds T,EUR,Value strategy,,Value Fund,Value Investm,DUS\n"
            "09AA.HM,Value Investm Klas Fds T,EUR,Value strategy,,Value Fund,Value Investm,HAM\n"
            "09AA.MU,Value Investm Klas Fds T,EUR,Value strategy,,Value Fund,Value Investm,MUN\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)
        self.assertEqual(inserted, 4)
        self.assertEqual(skipped, 0)

        rows = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_type='etf';"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "etf:09aa")

        listed = self.store.connection.execute(
            "SELECT dst_entity_id FROM entity_entity_map WHERE src_entity_id='etf:09aa' AND relation='listed_on' ORDER BY dst_entity_id;"
        ).fetchall()
        self.assertEqual({row[0] for row in listed}, {"mic:xber", "mic:xdus", "mic:xham", "mic:xmun"})

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
