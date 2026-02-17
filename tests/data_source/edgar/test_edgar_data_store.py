from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from config import Config
from data_sources.edgar.edgar_data_store import EdgarDataStore


class EdgarDataStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.store = EdgarDataStore(self.cfg)

    def tearDown(self) -> None:
        self.store.close()
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_uses_edgar_sqlite_filename(self) -> None:
        self.assertEqual(self.store.db_path.name, "edgar.sqlite")

    def test_records_submissions(self) -> None:
        payload = {"foo": "bar"}
        self.store.record_submissions(
            "0000123456",
            "Example Inc.",
            payload,
            fetched_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )

        row = self.store.connection.execute(
            "SELECT cik, entity_name, fetched_at, payload FROM edgar_submissions"
        ).fetchone()
        self.assertEqual(row[0], "0000123456")
        self.assertEqual(row[1], "Example Inc.")
        self.assertTrue(str(row[2]).startswith("2024-01-01T"))
        self.assertEqual(json.loads(row[3]), payload)

    def test_records_accession_files_and_payloads(self) -> None:
        files = ["a.htm", "b.pdf"]
        self.store.record_accession_index(
            "0000123456",
            "0000123456-00-000001",
            "https://example.com/edgar/",
            files,
            fetched_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )

        row = self.store.connection.execute("SELECT base_url, file_count FROM edgar_accession").fetchone()
        self.assertEqual(row[0], "https://example.com/edgar/")
        self.assertEqual(row[1], 2)

        fetched = self.store.get_accession_files("0000123456-00-000001")
        self.assertEqual(set(fetched), set(files))
        self.assertEqual(self.store.get_accession_base_url("0000123456-00-000001"), "https://example.com/edgar/")
        self.assertIsNone(self.store.get_accession_base_url("nope"))

        payload = b"hello world"
        self.store.store_file(
            "0000123456-00-000001",
            "a.htm",
            payload,
            fetched_at=datetime(2024, 1, 3, tzinfo=timezone.utc),
        )
        self.assertTrue(self.store.has_file("0000123456-00-000001", "a.htm"))
        self.assertEqual(self.store.get_file("0000123456-00-000001", "a.htm"), payload)
        info = self.store.get_accession_files_info("0000123456-00-000001")
        self.assertEqual(
            set(info),
            {
                ("a.htm", "https://example.com/edgar/a.htm"),
                ("b.pdf", "https://example.com/edgar/b.pdf"),
            },
        )

        self.assertTrue(self.store.has_accession("0000123456-00-000001"))
        self.assertTrue(self.store.has_accession("0000123456-00-000001", cik="0000123456"))
        self.assertFalse(self.store.has_accession("0000123456-00-000002"))
        self.assertEqual(self.store.known_accessions("0000123456"), {"0000123456-00-000001"})

    def test_stores_source_url_override(self) -> None:
        self.store.record_accession_index(
            "0000123456",
            "0000123456-00-000002",
            "https://example.com/edgar/",
            ["a.pdf"],
        )
        payload = b"pdf"
        self.store.store_file(
            "0000123456-00-000002",
            "a.pdf",
            payload,
            source_url="https://example.com/edgar/a.pdf",
        )
        info = self.store.get_accession_files_info("0000123456-00-000002")
        self.assertEqual(info, [("a.pdf", "https://example.com/edgar/a.pdf")])

    def test_ingests_xbrl_facts_minimal_instance(self) -> None:
        cik = "0000123456"
        accession = "0000123456-00-000003"
        self.store.record_accession_index(cik, accession, "https://example.com/edgar/", [])

        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:us-gaap="http://fasb.org/us-gaap/2024"
            xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <xbrli:context id="C1">
    <xbrli:entity>
      <xbrli:identifier scheme="http://www.sec.gov/CIK">0000123456</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:instant>2024-12-31</xbrli:instant>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="U1">
    <xbrli:measure>iso4217:USD</xbrli:measure>
  </xbrli:unit>
  <us-gaap:Assets contextRef="C1" unitRef="U1" decimals="-3">123000</us-gaap:Assets>
</xbrli:xbrl>
"""
        inserted = self.store.ingest_xbrl_facts(cik, accession, xml_bytes)
        self.assertEqual(inserted, 1)

        ctx_rows = self.store.connection.execute(
            "SELECT context_ref, period_type, instant_date, entity_scheme, entity_id FROM xbrl_context WHERE accession = ?",
            (accession,),
        ).fetchall()
        self.assertEqual(len(ctx_rows), 1)
        self.assertEqual(ctx_rows[0][0], "C1")
        self.assertEqual(ctx_rows[0][1], "instant")
        self.assertEqual(ctx_rows[0][2], "2024-12-31")
        self.assertEqual(ctx_rows[0][3], "http://www.sec.gov/CIK")
        self.assertEqual(ctx_rows[0][4], "0000123456")

        fact_rows = self.store.connection.execute(
            "SELECT value_numeric, value_raw, is_nil FROM xbrl_fact WHERE accession = ?",
            (accession,),
        ).fetchall()
        self.assertEqual(len(fact_rows), 1)
        self.assertEqual(fact_rows[0][0], 123000.0)
        self.assertEqual(fact_rows[0][1], "123000")
        self.assertEqual(fact_rows[0][2], 0)


if __name__ == "__main__":
    unittest.main()

