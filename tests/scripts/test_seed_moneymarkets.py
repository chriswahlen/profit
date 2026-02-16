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
        self.assertEqual(fund_id, "fund:spxmm")
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

    def test_same_money_market_multiple_exchanges_collapses(self) -> None:
        csv_path = Path(self.tmpdir.name) / "multi.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,family,exchange\n"
            "09AA.BE,Value Investm Klas Fds T,EUR,Value strategy,Value Investm,BER\n"
            "09AA.DU,Value Investm Klas Fds T,EUR,Value strategy,Value Investm,DUS\n"
            "09AA.HM,Value Investm Klas Fds T,EUR,Value strategy,Value Investm,HAM\n"
            "09AA.MU,Value Investm Klas Fds T,EUR,Value strategy,Value Investm,MUN\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)
        self.assertEqual(inserted, 4)
        self.assertEqual(skipped, 0)

        entries = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_type='fund';"
        ).fetchall()
        self.assertEqual(entries, [("fund:09aa",)])

        listed = self.store.connection.execute(
            "SELECT dst_entity_id FROM entity_entity_map WHERE src_entity_id='fund:09aa' AND relation='listed_on' ORDER BY dst_entity_id;"
        ).fetchall()
        self.assertEqual({row[0] for row in listed}, {"mic:xber", "mic:xdus", "mic:xham", "mic:xmun"})

    def test_metadata_difference_creates_alt_entity(self) -> None:
        csv_path = Path(self.tmpdir.name) / "diff.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,family,exchange\n"
            "09AA.BE,Value Investm Klas Fds T,EUR,Value strategy,Value Investm,BER\n"
            "09AA.BE,Value Investm Klas Fds T,CNY,Value strategy (CNY),Value Investm,BER\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)
        self.assertEqual(inserted, 2)
        self.assertEqual(skipped, 0)

        ids = {
            row[0]
            for row in self.store.connection.execute("SELECT entity_id FROM entities WHERE entity_type='fund';").fetchall()
        }
        self.assertEqual(ids, {"fund:09aa", "fund:09aa.be"})

    def test_skips_missing_symbol(self) -> None:
        rows = [{"symbol": "", "name": "", "currency": "", "summary": "", "family": "", "exchange": ""}]
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 0)
        self.assertEqual(skipped, 1)
