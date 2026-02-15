from __future__ import annotations

import os
import tempfile
from pathlib import Path
import unittest

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_sec_tickers import seed, SecRow


class SeedSecTickersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        os.environ["SEC_USER_AGENT"] = "test@example.com"
        self.cfg = Config()

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        os.environ.pop("SEC_USER_AGENT", None)
        self.tmpdir.cleanup()

    def test_seed_inserts_entities_and_mappings(self):
        rows = [
            SecRow(cik="0000320193", ticker="AAPL", name="Apple Inc."),
            SecRow(cik="0000789019", ticker="MSFT", name="Microsoft Corporation"),
        ]
        store = EntityStore(self.cfg)
        seed(rows, store)

        conn = store.connection
        cur = conn.execute("SELECT entity_id, name FROM entities ORDER BY entity_id;")
        data = cur.fetchall()
        self.assertEqual(data[0][0], "company:us:apple-inc")
        self.assertEqual(data[1][0], "company:us:microsoft-corporation")

        cur = conn.execute("SELECT provider, provider_entity_id, entity_id FROM provider_entity_map ORDER BY provider_entity_id;")
        mappings = cur.fetchall()
        self.assertEqual(mappings, [("provider:edgar", "0000320193", "company:us:apple-inc"), ("provider:edgar", "0000789019", "company:us:microsoft-corporation")])


if __name__ == "__main__":
    unittest.main()
