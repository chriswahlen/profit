from __future__ import annotations

import csv
import gzip
import os
import sqlite3
import tempfile
from pathlib import Path
from contextlib import contextmanager
import unittest

from config import Config
from data_sources.entity import EntityStore
from data_sources.redfin.redfin_data_source import RedfinDataSource


@contextmanager
def temp_cwd(path: Path):
    prev = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


class RedfinIngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.base = Path(self.tmpdir.name)
        self.data_path = self.base / "data"
        self.data_path.mkdir(parents=True, exist_ok=True)

        # Point config storage to temp data path.
        os.environ["PROFIT_DATA_PATH"] = str(self.data_path)

        # Prepare incoming dataset path and sample gz file.
        incoming = self.base / "incoming" / "datasets" / "redfin"
        incoming.mkdir(parents=True, exist_ok=True)
        self.sample_file = incoming / "sample.tsv000.gz"
        self._write_sample_file(self.sample_file)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()
        os.environ.pop("PROFIT_DATA_PATH", None)

    def _write_sample_file(self, path: Path) -> None:
        header = [
            "PERIOD_BEGIN",
            "PERIOD_END",
            "PERIOD_DURATION",
            "REGION_TYPE",
            "REGION_TYPE_ID",
            "TABLE_ID",
            "IS_SEASONALLY_ADJUSTED",
            "REGION",
            "CITY",
            "STATE",
            "STATE_CODE",
            "PROPERTY_TYPE",
            "PROPERTY_TYPE_ID",
            "MEDIAN_SALE_PRICE",
            "MEDIAN_LIST_PRICE",
            "MEDIAN_PPSF",
            "HOMES_SOLD",
            "NEW_LISTINGS",
            "INVENTORY",
            "MEDIAN_DOM",
            "AVG_SALE_TO_LIST",
            "PRICE_DROPS",
            "PENDING_SALES",
            "MONTHS_OF_SUPPLY",
            "PARENT_METRO_REGION",
            "PARENT_METRO_REGION_METRO_CODE",
            "LAST_UPDATED",
        ]
        rows = [
            {
                "PERIOD_BEGIN": "2025-01-01",
                "PERIOD_END": "2025-01-31",
                "PERIOD_DURATION": "30",
                "REGION_TYPE": "metro",
                "REGION_TYPE_ID": "-2",
                "TABLE_ID": "12345",
                "IS_SEASONALLY_ADJUSTED": "false",
                "REGION": "Test Metro",
                "CITY": "",
                "STATE": "Texas",
                "STATE_CODE": "TX",
                "PROPERTY_TYPE": "All Residential",
                "PROPERTY_TYPE_ID": "-1",
                "MEDIAN_SALE_PRICE": "100000",
                "MEDIAN_LIST_PRICE": "120000",
                "MEDIAN_PPSF": "150",
                "HOMES_SOLD": "10",
                "NEW_LISTINGS": "12",
                "INVENTORY": "50",
                "MEDIAN_DOM": "20",
                "AVG_SALE_TO_LIST": "0.98",
                "PRICE_DROPS": "0.05",
                "PENDING_SALES": "8",
                "MONTHS_OF_SUPPLY": "4.0",
                "PARENT_METRO_REGION": "",
                "PARENT_METRO_REGION_METRO_CODE": "",
                "LAST_UPDATED": "2026-01-12 14:43:38.223 Z",
            },
            {
                "PERIOD_BEGIN": "2025-01-01",
                "PERIOD_END": "2025-01-31",
                "PERIOD_DURATION": "30",
                "REGION_TYPE": "metro",
                "REGION_TYPE_ID": "-2",
                "TABLE_ID": "12345",
                "IS_SEASONALLY_ADJUSTED": "false",
                "REGION": "Test Metro",
                "CITY": "",
                "STATE": "Texas",
                "STATE_CODE": "TX",
                "PROPERTY_TYPE": "Townhouse",
                "PROPERTY_TYPE_ID": "4",
                "MEDIAN_SALE_PRICE": "100000",
                "MEDIAN_LIST_PRICE": "120000",
                "MEDIAN_PPSF": "150",
                "HOMES_SOLD": "10",
                "NEW_LISTINGS": "12",
                "INVENTORY": "50",
                "MEDIAN_DOM": "20",
                "AVG_SALE_TO_LIST": "0.98",
                "PRICE_DROPS": "0.05",
                "PENDING_SALES": "8",
                "MONTHS_OF_SUPPLY": "4.0",
                "PARENT_METRO_REGION": "",
                "PARENT_METRO_REGION_METRO_CODE": "",
                "LAST_UPDATED": "2026-01-12 14:43:38.223 Z",
            },
        ]
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=header, delimiter="\t", quotechar='"')
            writer.writeheader()
            writer.writerows(rows)

    def test_ingest_all_residential_only(self) -> None:
        cfg = Config()
        entity_store = EntityStore(cfg)
        source = RedfinDataSource(cfg, entity_store)

        with temp_cwd(self.base):
            res = source.ensure_up_to_date([])
        self.assertEqual(res.failed, 0)
        self.assertEqual(res.updated, 2)  # two property-type rows ingested

        db_path = Path(cfg.data_path()) / "redfin_metrics.sqlite"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM market_metrics;")
        self.assertEqual(cur.fetchone()[0], 2)

        cur.execute("SELECT COUNT(*) FROM regions;")
        self.assertEqual(cur.fetchone()[0], 1)

        # Verify provider map captured TABLE_ID.
        cur.execute("SELECT provider_region_id FROM region_provider_map;")
        self.assertEqual(cur.fetchone()[0], "12345")

        # Ingestion run recorded.
        cur.execute("SELECT status, row_count FROM ingestion_runs;")
        status, row_count = cur.fetchone()
        self.assertEqual(status, "success")
        self.assertEqual(row_count, 2)

        conn.close()

    def test_reimport_is_idempotent(self) -> None:
        cfg = Config()
        entity_store = EntityStore(cfg)
        source = RedfinDataSource(cfg, entity_store)

        with temp_cwd(self.base):
            first = source.ensure_up_to_date([])
            second = source.ensure_up_to_date([])

        self.assertEqual(first.failed, 0)
        self.assertEqual(second.failed, 0)

        db_path = Path(cfg.data_path()) / "redfin_metrics.sqlite"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM market_metrics;")
        self.assertEqual(cur.fetchone()[0], 2)

        cur.execute("SELECT COUNT(*) FROM regions;")
        self.assertEqual(cur.fetchone()[0], 1)

        conn.close()

    def test_neighborhood_canonical_id(self) -> None:
        original_data_path = os.environ.get("PROFIT_DATA_PATH")
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["PROFIT_DATA_PATH"] = str(Path(tmpdir) / "data")
            base = Path(tmpdir)
            incoming = base / "incoming/datasets/redfin"
            incoming.mkdir(parents=True, exist_ok=True)
            sample = incoming / "neighborhood.tsv000.gz"
            header = [
                "PERIOD_BEGIN",
                "PERIOD_END",
                "PERIOD_DURATION",
                "REGION_TYPE",
                "REGION_TYPE_ID",
                "TABLE_ID",
                "IS_SEASONALLY_ADJUSTED",
                "REGION",
                "CITY",
                "STATE",
                "STATE_CODE",
                "PROPERTY_TYPE",
                "PROPERTY_TYPE_ID",
                "MEDIAN_SALE_PRICE",
                "MEDIAN_LIST_PRICE",
                "MEDIAN_PPSF",
                "HOMES_SOLD",
                "NEW_LISTINGS",
                "INVENTORY",
                "MEDIAN_DOM",
                "AVG_SALE_TO_LIST",
                "PRICE_DROPS",
                "PENDING_SALES",
                "MONTHS_OF_SUPPLY",
                "PARENT_METRO_REGION",
                "PARENT_METRO_REGION_METRO_CODE",
                "LAST_UPDATED",
            ]
            row = {
                "PERIOD_BEGIN": "2025-01-01",
                "PERIOD_END": "2025-01-31",
                "PERIOD_DURATION": "30",
                "REGION_TYPE": "neighborhood",
                "REGION_TYPE_ID": "123",
                "TABLE_ID": "99999",
                "IS_SEASONALLY_ADJUSTED": "false",
                "REGION": "Ballard",
                "CITY": "Seattle",
                "STATE": "Washington",
                "STATE_CODE": "WA",
                "PROPERTY_TYPE": "All Residential",
                "PROPERTY_TYPE_ID": "-1",
                "MEDIAN_SALE_PRICE": "750000",
                "MEDIAN_LIST_PRICE": "800000",
                "MEDIAN_PPSF": "500",
                "HOMES_SOLD": "20",
                "NEW_LISTINGS": "30",
                "INVENTORY": "60",
                "MEDIAN_DOM": "15",
                "AVG_SALE_TO_LIST": "0.99",
                "PRICE_DROPS": "0.02",
                "PENDING_SALES": "18",
                "MONTHS_OF_SUPPLY": "3.2",
                "PARENT_METRO_REGION": "",
                "PARENT_METRO_REGION_METRO_CODE": "",
                "LAST_UPDATED": "2026-01-12 14:43:38.223 Z",
            }
            with gzip.open(sample, "wt", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=header, delimiter="\t", quotechar='"')
                writer.writeheader()
                writer.writerow(row)

            cfg = Config()
            entity_store = EntityStore(cfg)
            source = RedfinDataSource(cfg, entity_store)
            with temp_cwd(base):
                res = source.ensure_up_to_date([])
            self.assertEqual(res.failed, 0)
            db_path = Path(cfg.data_path()) / "redfin_metrics.sqlite"
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT region_id FROM regions")
            region_id = cur.fetchone()[0]
            self.assertEqual(region_id, "region:neighborhood:us:wa:seattle:ballard")
            conn.close()
        if original_data_path is not None:
            os.environ["PROFIT_DATA_PATH"] = original_data_path
        else:
            os.environ.pop("PROFIT_DATA_PATH", None)


if __name__ == "__main__":
    unittest.main()
