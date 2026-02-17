from __future__ import annotations

import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import seed_cli
from config import Config
from data_sources.edgar.edgar_data_store import EdgarDataStore


class SeedCliSeedEdgarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_edgar_writes_edgar_sqlite(self) -> None:
        # Create a tiny submissions bundle.
        zip_path = Path(self.tmpdir.name) / "submissions.zip"
        payload = {"cik": "0000000001", "name": "Example Inc.", "filings": {"recent": {}}}
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("CIK0000000001.json", json.dumps(payload))

        argv = ["seed_cli", "seed-edgar", "--submissions-zip", str(zip_path), "1"]
        with mock.patch("sys.argv", argv):
            rc = seed_cli.main()
        self.assertEqual(rc, 0)

        store = EdgarDataStore(Config())
        try:
            row = store.connection.execute("SELECT cik, entity_name FROM edgar_submissions").fetchone()
            self.assertEqual(row[0], "0000000001")
            self.assertEqual(row[1], "Example Inc.")
            self.assertEqual(store.db_path.name, "edgar.sqlite")
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()

