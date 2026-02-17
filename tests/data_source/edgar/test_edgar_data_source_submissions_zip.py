from __future__ import annotations

import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path

from config import Config
from data_sources.edgar.edgar_data_source import EdgarDataSource, SEC_PROVIDER
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.entity import Entity, EntityStore, EntityType


class EdgarDataSourceSubmissionsZipTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.entity_store = EntityStore(self.cfg)
        self.edgar_store = EdgarDataStore(self.cfg)

    def tearDown(self) -> None:
        self.entity_store.close()
        self.edgar_store.close()
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_ingests_submissions_for_mapped_cik(self) -> None:
        # Set up canonical entity + SEC provider mapping.
        entity_id = "company:us:example-inc"
        self.entity_store.upsert_provider(provider=SEC_PROVIDER, description="SEC EDGAR", base_url=None)
        self.entity_store.upsert_entity(Entity(entity_id=entity_id, entity_type=EntityType.COMPANY, name="Example Inc."), overwrite=True)
        self.entity_store.map_provider_entity(provider=SEC_PROVIDER, provider_entity_id="0000000001", entity_id=entity_id)

        # Create a tiny submissions bundle with a main payload and one paged payload.
        zip_path = Path(self.tmpdir.name) / "submissions.zip"
        main_payload = {
            "cik": "0000000001",
            "name": "Example Inc.",
            "filings": {"recent": {"accessionNumber": ["0000000001-24-000002"], "form": ["10-Q"], "filingDate": ["2024-09-30"]}},
        }
        page_payload = {
            "filings": {"recent": {"accessionNumber": ["0000000001-24-000001"], "form": ["10-K"], "filingDate": ["2024-02-15"]}}
        }
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("CIK0000000001.json", json.dumps(main_payload))
            zf.writestr("CIK0000000001-submissions-001.json", json.dumps(page_payload))

        src = EdgarDataSource(
            self.cfg,
            entity_store=self.entity_store,
            store=self.edgar_store,
            submissions_zip_path=zip_path,
        )
        res = src.ensure_up_to_date([entity_id])
        self.assertEqual(res.failed, 0)
        self.assertEqual(res.updated, 1)

        row = self.edgar_store.connection.execute(
            "SELECT cik, entity_name, payload FROM edgar_submissions WHERE cik = ?",
            ("0000000001",),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "0000000001")
        self.assertEqual(row[1], "Example Inc.")
        payload = json.loads(row[2])
        self.assertEqual(payload["name"], "Example Inc.")
        self.assertEqual(len(payload.get("__profit2_paged_payloads", [])), 1)


if __name__ == "__main__":
    unittest.main()

