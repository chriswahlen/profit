from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_financedatabase import (
    seed_rows,
    load_csv,
    RELATION_COMPANY_ISSUER,
    RELATION_SECTOR,
    RELATION_INDUSTRY,
    ISIN_PROVIDER,
)


class FinanceDatabaseSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_rows_inserts_entities_and_provider_maps(self):
        csv_path = Path(self.tmpdir.name) / "sample.csv"
        csv_path.write_text(
            "symbol,name,exchange,currency,country,sector,industry,isin\n"
            "AAPL,Apple Inc.,NMS,USD,United States,Technology,Consumer Electronics,US0378331005\n"
            "MSFT,Microsoft Corp,NYQ,USD,United States,Technology,Software,US5949181045\n"
            "BAD,Missing Exchange,ZZZ,USD,United States,Technology,Software,\n"
        )

        rows = load_csv(csv_path)
        inserted, skipped = seed_rows(rows, self.store)

        self.assertEqual(inserted, 2)
        self.assertEqual(skipped, 1)

        cur = self.store.connection.execute("SELECT entity_id, name FROM entities WHERE entity_id LIKE 'sec:%' ORDER BY entity_id;")
        ids = cur.fetchall()
        self.assertEqual(ids, [("sec:xnas:aapl", "Apple Inc."), ("sec:xnys:msft", "Microsoft Corp")])

        cur = self.store.connection.execute(
            "SELECT provider, provider_entity_id, entity_id, metadata FROM provider_entity_map ORDER BY provider_entity_id;"
        )
        maps = cur.fetchall()
        self.assertIn(
            ("provider:financedatabase", "AAPL", "sec:xnas:aapl", None),
            maps,
        )
        self.assertIn(
            ("provider:financedatabase", "MSFT", "sec:xnys:msft", None),
            maps,
        )
        isin_rows = self.store.connection.execute(
            "SELECT provider_entity_id, entity_id FROM provider_entity_map WHERE provider=? ORDER BY entity_id;",
            (ISIN_PROVIDER,),
        ).fetchall()
        self.assertEqual(
            set(isin_rows),
            {
                ("US0378331005", "company:us:apple-inc"),
                ("US5949181045", "company:us:microsoft-corp"),
            },
        )
        cur = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_id LIKE 'company:%' ORDER BY entity_id;"
        )
        companies = [row[0] for row in cur.fetchall()]
        self.assertEqual(companies, ["company:us:apple-inc", "company:us:microsoft-corp"])

        rel_cur = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id, relation FROM entity_entity_map WHERE relation='issued_security' ORDER BY src_entity_id;"
        )
        relations = rel_cur.fetchall()
        self.assertEqual(
            relations,
            [
                ("company:us:apple-inc", "sec:xnas:aapl", "issued_security"),
                ("company:us:microsoft-corp", "sec:xnys:msft", "issued_security"),
            ],
        )

        sector_rows = self.store.connection.execute(
            "SELECT entity_id, name FROM entities WHERE entity_id LIKE 'sector:%' ORDER BY entity_id;"
        ).fetchall()
        self.assertTrue(sector_rows)
        industry_rows = self.store.connection.execute(
            "SELECT entity_id, name FROM entities WHERE entity_id LIKE 'industry:%' ORDER BY entity_id;"
        ).fetchall()
        self.assertTrue(industry_rows)

        sector_map = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id, relation FROM entity_entity_map WHERE relation=? ORDER BY src_entity_id;",
            (RELATION_SECTOR,),
        ).fetchall()
        industry_map = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id, relation FROM entity_entity_map WHERE relation=? ORDER BY src_entity_id;",
            (RELATION_INDUSTRY,),
        ).fetchall()
        self.assertTrue(sector_map)
        self.assertTrue(industry_map)

    def test_skips_company_when_name_missing(self):
        csv_path = Path(self.tmpdir.name) / "sample_missing_name.csv"
        csv_path.write_text(
            "symbol,name,exchange,currency,country,sector,industry,isin\n"
            "ETFXYZ,,NMS,USD,United States,Financials,Investment Services,\n"
        )
        rows = load_csv(csv_path)
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 1)
        self.assertEqual(skipped, 0)

        cur = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_id LIKE 'company:%';"
        )
        self.assertEqual(cur.fetchall(), [])


if __name__ == "__main__":
    unittest.main()
