from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import json

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_indices import rows_from_csv, seed_rows


class IndexSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_rows_creates_indexes_and_relations(self) -> None:
        csv_path = Path(self.tmpdir.name) / "indices.csv"
        csv_path.write_text(
            "symbol,name,currency,summary,category_group,category,exchange\n"
            "000123.SS,Sample Shanghai Index,CNY,Tracks innovators,Equities,Large Cap,SHH\n"
            "SPX,SPX Index,USD,Tracks S&P 500,Equities,Large Cap,NYS\n"
        )

        rows = list(rows_from_csv(csv_path))
        inserted, skipped = seed_rows(rows, self.store, progress_interval=1)

        self.assertEqual(inserted, 2)
        self.assertEqual(skipped, 0)

        index_rows = self.store.connection.execute(
            "SELECT entity_id, entity_type, metadata FROM entities WHERE entity_type='index' ORDER BY entity_id;"
        ).fetchall()
        self.assertEqual(len(index_rows), 2)
        ids = {row[0] for row in index_rows}
        self.assertEqual(ids, {"index:xshg:000123ss", "index:xnys:spx"})
        metadata = {row[0]: json.loads(row[2]) for row in index_rows}
        self.assertEqual(metadata["index:xshg:000123ss"]["currency"], "ccy:cny")
        self.assertEqual(metadata["index:xshg:000123ss"]["summary"], "Tracks innovators")
        self.assertEqual(metadata["index:xshg:000123ss"]["category_group"], "Equities")

        provider_map = self.store.connection.execute(
            "SELECT provider_entity_id FROM provider_entity_map WHERE provider='provider:financedatabase' ORDER BY provider_entity_id;"
        ).fetchall()
        self.assertEqual([row[0] for row in provider_map], ["000123.SS", "SPX"])

        listed_relations = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id, relation FROM entity_entity_map WHERE relation='listed_on' ORDER BY src_entity_id;"
        ).fetchall()
        self.assertEqual(len(listed_relations), 2)
        dests = {row[1] for row in listed_relations}
        self.assertEqual(dests, {"mic:xshg", "mic:xnys"})

    def test_skips_rows_without_symbol(self) -> None:
        rows = [{"symbol": "", "name": "", "currency": "", "summary": "", "category_group": "", "category": "", "exchange": ""}]
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 0)
        self.assertEqual(skipped, 1)
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM entities WHERE entity_type='index';").fetchone()[0],
            0,
        )

    def test_duplicate_symbol_rows_create_single_entity(self) -> None:
        rows = [
            {"symbol": "SPX", "name": "SPX Index", "currency": "USD", "summary": "S&P 500", "category_group": "Equities", "category": "Large Cap", "exchange": "NYS"},
            {"symbol": "SPX", "name": "SPX Index", "currency": "USD", "summary": "S&P 500 Duplicate", "category_group": "Equities", "category": "Large Cap", "exchange": "NYS"},
        ]
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 1)
        self.assertEqual(skipped, 0)
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM entities WHERE entity_type='index';").fetchone()[0],
            1,
        )
        provider_count = self.store.connection.execute(
            "SELECT COUNT(*) FROM provider_entity_map WHERE provider='provider:financedatabase';"
        ).fetchone()[0]
        self.assertEqual(provider_count, 1)


if __name__ == "__main__":
    unittest.main()
