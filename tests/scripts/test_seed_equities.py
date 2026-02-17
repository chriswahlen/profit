from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import json

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_equities import (
    seed_rows,
    load_csv,
    RELATION_COMPANY_ISSUER,
    RELATION_SECTOR,
    RELATION_INDUSTRY,
    ISIN_PROVIDER,
    CUSIP_PROVIDER,
    FIGI_PROVIDER,
)


class EquitiesSeedTests(unittest.TestCase):
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
        self.assertEqual(
            ids,
            [
                ("sec:isin:us0378331005", "Apple Inc."),
                ("sec:isin:us5949181045", "Microsoft Corp"),
            ],
        )

        cur = self.store.connection.execute(
            "SELECT provider, provider_entity_id, entity_id, metadata FROM provider_entity_map ORDER BY provider_entity_id;"
        )
        maps = cur.fetchall()
        self.assertIn(
            ("provider:financedatabase", "AAPL", "sec:isin:us0378331005", None),
            maps,
        )
        self.assertIn(
            ("provider:financedatabase", "MSFT", "sec:isin:us5949181045", None),
            maps,
        )
        isin_rows = self.store.connection.execute(
            "SELECT provider_entity_id, entity_id FROM provider_entity_map WHERE provider=? ORDER BY entity_id;",
            (ISIN_PROVIDER,),
        ).fetchall()
        self.assertEqual(
            set(isin_rows),
            {
                ("US0378331005", "sec:isin:us0378331005"),
                ("US5949181045", "sec:isin:us5949181045"),
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
                ("company:us:apple-inc", "sec:isin:us0378331005", "issued_security"),
                ("company:us:microsoft-corp", "sec:isin:us5949181045", "issued_security"),
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

    def test_stores_metadata_identifiers(self) -> None:
        csv_path = Path(self.tmpdir.name) / "metadata.csv"
        csv_path.write_text(
            "symbol,name,exchange,currency,country,sector,industry,isin,cusip,figi,composite_figi,shareclass_figi,summary\n"
            "AAPL,Apple Inc.,NMS,USD,United States,Technology,Consumer Electronics,US0378331005,037833100,BBG000B9XRY4,BBG000B9XRY4,BBG000B9XRY4,Leading consumer tech brand\n"
        )

        rows = load_csv(csv_path)
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 1)
        self.assertEqual(skipped, 0)

        metadata = self.store.connection.execute(
            "SELECT metadata FROM entities WHERE entity_id='sec:isin:us0378331005';"
        ).fetchone()[0]
        self.assertEqual(
            json.loads(metadata),
            {
                "summary": "Leading consumer tech brand",
                "isin": "US0378331005",
                "cusip": "037833100",
                "figi": "BBG000B9XRY4",
                "composite_figi": "BBG000B9XRY4",
                "shareclass_figi": "BBG000B9XRY4",
            },
        )

        provider_ids = self.store.connection.execute(
            "SELECT provider, provider_entity_id FROM provider_entity_map WHERE entity_id='sec:isin:us0378331005';"
        ).fetchall()
        self.assertIn((CUSIP_PROVIDER, "037833100"), provider_ids)
        self.assertIn((FIGI_PROVIDER, "BBG000B9XRY4"), provider_ids)

    def test_rows_without_isin_follow_company_back_to_isin(self) -> None:
        csv_path = Path(self.tmpdir.name) / "company.csv"
        csv_path.write_text(
            "symbol,name,exchange,country,sector,industry,isin\n"
            "TEST1,Mock Corp,NMS,United States,Technology,Software,US1234567890\n"
            "SYMBOLX,Mock Corp,NMS,United States,Technology,Software,\n"
        )

        rows = load_csv(csv_path)
        inserted, skipped = seed_rows(rows, self.store)

        self.assertEqual(inserted, 1)
        self.assertEqual(skipped, 0)

        entity_rows = self.store.connection.execute("SELECT entity_id FROM entities WHERE entity_id LIKE 'sec:%';").fetchall()
        self.assertEqual(entity_rows, [("sec:isin:us1234567890",)])

        provider_rows = self.store.connection.execute(
            "SELECT provider_entity_id, entity_id FROM provider_entity_map WHERE provider='provider:financedatabase' ORDER BY provider_entity_id;"
        ).fetchall()
        self.assertEqual(
            provider_rows,
            [("SYMBOLX", "sec:isin:us1234567890"), ("TEST1", "sec:isin:us1234567890")],
        )

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

    def test_creates_fund_entity_for_fund_name(self):
        csv_path = Path(self.tmpdir.name) / "fund.csv"
        csv_path.write_text(
            "symbol,name,exchange,currency,country,sector,industry,isin\n"
            "BRES,Barwa Real Estate Company Q.P.S.C.,DOH,QAR,Qatar,Real Estate,Real Estate,\n"
        )

        rows = load_csv(csv_path)
        inserted, skipped = seed_rows(rows, self.store)
        self.assertEqual(inserted, 1)
        self.assertEqual(skipped, 0)

        fund_entities = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_id LIKE 'fund_entity:%';"
        ).fetchall()
        self.assertEqual(fund_entities, [("fund_entity:barwa-real-estate-qpsc",)])

        relations = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id FROM entity_entity_map WHERE relation='issued_security';"
        ).fetchall()
        self.assertTrue(relations)
        self.assertEqual(relations[0][0], "fund_entity:barwa-real-estate-qpsc")

    def test_fund_entity_name_dedupes_across_variants(self):
        csv_path = Path(self.tmpdir.name) / "fund_dupes.csv"
        csv_path.write_text(
            "symbol,name,exchange,currency,country,sector,industry,isin\n"
            "BRES,Barwa Real Estate Company Q.P.S.C.,DOH,QAR,Qatar,Real Estate,Real Estate,\n"
            "BRES2,Barwa Real Estate co. Q.P.S.C.,DOH,QAR,Qatar,Real Estate,Real Estate,\n"
        )

        rows = load_csv(csv_path)
        seed_rows(rows, self.store)

        fund_entities = self.store.connection.execute(
            "SELECT entity_id FROM entities WHERE entity_id LIKE 'fund_entity:%';"
        ).fetchall()
        self.assertEqual(fund_entities, [("fund_entity:barwa-real-estate-qpsc",)])

        relations = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id FROM entity_entity_map WHERE relation='issued_security' ORDER BY dst_entity_id;"
        ).fetchall()
        self.assertEqual(relations[0][0], "fund_entity:barwa-real-estate-qpsc")
        self.assertEqual(relations[1][0], "fund_entity:barwa-real-estate-qpsc")


if __name__ == "__main__":
    unittest.main()
