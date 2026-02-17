from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fetch_cli
from config import Config
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.edgar.http import FetchResponse


class FetchCliFetchEdgarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        os.environ["SEC_USER_AGENT"] = "test@example.com"

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        os.environ.pop("SEC_USER_AGENT", None)
        self.tmpdir.cleanup()

    def test_fetch_edgar_writes_edgar_sqlite_and_includes_pages(self) -> None:
        requested_urls: list[str] = []

        main_payload = {
            "cik": "0000000001",
            "name": "Example Inc.",
            "filings": {
                "recent": {"accessionNumber": [], "form": [], "filingDate": []},
                "files": [{"name": "CIK0000000001-001.json"}],
            },
        }
        page_payload = {
            "filings": {"recent": {"accessionNumber": ["0000000001-24-000001"], "form": ["10-K"], "filingDate": ["2024-02-15"]}}
        }

        def fake_fetch(url: str, *, timeout: float, headers: dict[str, str]):
            requested_urls.append(url)
            if url.endswith("CIK0000000001.json"):
                body = json.dumps(main_payload).encode()
            elif url.endswith("CIK0000000001-001.json"):
                body = json.dumps(page_payload).encode()
            else:
                raise AssertionError(f"unexpected url {url}")
            return FetchResponse(status=200, body=body, headers={})

        argv = ["fetch_cli", "fetch-edgar", "1"]
        with mock.patch("sys.argv", argv), mock.patch("data_sources.edgar.http.default_fetch", side_effect=fake_fetch):
            rc = fetch_cli.main()
        self.assertEqual(rc, 0)

        store = EdgarDataStore(Config())
        try:
            row = store.connection.execute("SELECT cik, entity_name, payload FROM edgar_submissions").fetchone()
            self.assertEqual(row[0], "0000000001")
            self.assertEqual(row[1], "Example Inc.")
            payload = json.loads(row[2])
            self.assertEqual(len(payload.get("__profit2_paged_payloads", [])), 1)
            self.assertEqual(store.db_path.name, "edgar.sqlite")
        finally:
            store.close()

        self.assertEqual(
            requested_urls,
            [
                "https://data.sec.gov/submissions/CIK0000000001.json",
                "https://data.sec.gov/submissions/CIK0000000001-001.json",
            ],
        )


if __name__ == "__main__":
    unittest.main()

