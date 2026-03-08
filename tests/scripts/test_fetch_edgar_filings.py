from __future__ import annotations

import io
import os
import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path
from typing import Dict, Tuple

from config import Config
from data_sources.edgar.accession_reader import AccessionIndex
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.edgar.sec_edgar import EdgarFiling, EdgarSubmissions
from scripts.fetch_edgar import ingest_recent_filings, _prepare_payload_for_ingest


class _StubReader:
    def __init__(self, indexes: Dict[Tuple[str, str], AccessionIndex], files: Dict[Tuple[str, str, str], bytes]):
        self.indexes = indexes
        self.files = files
        self.fetch_calls: list[Tuple[str, str, str]] = []

    def fetch_index(self, cik: str, accession: str) -> AccessionIndex:
        return self.indexes[(cik, accession)]

    def fetch_file(self, cik: str, accession: str, filename: str) -> bytes:
        self.fetch_calls.append((cik, accession, filename))
        return self.files[(cik, accession, filename)]


def _simple_xbrl(amount: int) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:us-gaap="http://fasb.org/us-gaap/2024"
            xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <xbrli:context id="C1">
    <xbrli:entity>
      <xbrli:identifier scheme="http://www.sec.gov/CIK">0000000001</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:instant>2024-12-31</xbrli:instant>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="U1">
    <xbrli:measure>iso4217:USD</xbrli:measure>
  </xbrli:unit>
  <us-gaap:Assets contextRef="C1" unitRef="U1" decimals="-3">{amount}</us-gaap:Assets>
</xbrli:xbrl>
""".encode("utf-8")


def _make_zip(payloads: Dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zin:
        for name, data in payloads.items():
            zin.writestr(name, data)
    return buffer.getvalue()


class FetchEdgarFilingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        os.environ["SEC_USER_AGENT"] = "test@example.com"
        self.cfg = Config()
        self.store = EdgarDataStore(self.cfg)

    def tearDown(self) -> None:
        self.store.close()
        os.environ.pop("PROFIT_DATA_PATH", None)
        os.environ.pop("SEC_USER_AGENT", None)
        self.tmpdir.cleanup()

    def test_ingests_xml_and_zip_files(self) -> None:
        cik = "0000000001"
        accession_raw = "0000000001-24-000001"
        normalized_accession = "000000000124000001"

        xml_one = _simple_xbrl(100)
        xml_two = _simple_xbrl(200)
        zipped_payload = _make_zip({"inner-instance.xml": xml_two})

        index = AccessionIndex(
            base_url="https://example.com/edgar/",
            files=[
                {"name": "inst1.xml"},
                {"name": "filingsummary.xml"},
                {"name": "xbrl.zip"},
            ],
            raw={},
        )

        reader = _StubReader(
            indexes={(cik, normalized_accession): index},
            files={
                (cik, normalized_accession, "inst1.xml"): xml_one,
                (cik, normalized_accession, "xbrl.zip"): zipped_payload,
            },
        )

        filings = [
            EdgarFiling(
                accession_number=accession_raw,
                form="10-K",
                filing_date=date(2024, 2, 15),
                primary_document="primary.htm",
                report_date=None,
            )
        ]
        submissions = [
            EdgarSubmissions(
                cik=cik,
                entity_name="Example",
                recent_filings=filings,
                raw={},
            )
        ]

        result = ingest_recent_filings(
            submissions=submissions,
            store=self.store,
            user_agent="test@example.com",
            pause_s=0.0,
            accession_reader=reader,
            log_each_accession=False,
        )

        self.assertEqual(result.accessions, 1)
        self.assertEqual(result.files, 2)
        # ingest_xbrl_facts stops after the first successful ingestion for the accession,
        # so the zipped payload is skipped once facts already exist.
        self.assertEqual(result.facts, 1)
        self.assertEqual(result.failed, 0)

        self.assertIn(
            (cik, normalized_accession, "inst1.xml"),
            reader.fetch_calls,
        )
        self.assertIn(
            (cik, normalized_accession, "xbrl.zip"),
            reader.fetch_calls,
        )
        self.assertNotIn((cik, normalized_accession, "filingsummary.xml"), reader.fetch_calls)

        self.assertEqual(self.store.get_file(normalized_accession, "inst1.xml"), xml_one)
        self.assertEqual(self.store.get_file(normalized_accession, "xbrl.zip"), zipped_payload)
        self.assertTrue(self.store.has_accession(normalized_accession))

        fact_count = self.store.connection.execute(
            "SELECT COUNT(*) FROM xbrl_fact WHERE accession = ?", (normalized_accession,)
        ).fetchone()[0]
        self.assertEqual(fact_count, 1)

    def test_trim_sgml_header_before_ingest(self) -> None:
        payload = (
            b"SEC-DOCUMENT>0001780525-26-000003.txt : 20260306\n"
            b"<SEC-HEADER>something\n"
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<xbrli:xbrl><xbrli:context><xbrli:entity>"
            b"</xbrli:entity></xbrli:context></xbrli:xbrl>\n"
        )
        trimmed = _prepare_payload_for_ingest(payload)
        self.assertTrue(trimmed.startswith(b"<?xml"))
