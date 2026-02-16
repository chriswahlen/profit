from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_financedatabase import seed_rows, load_csv


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

        cur = self.store.connection.execute("SELECT entity_id, name FROM entities ORDER BY entity_id;")
        ids = cur.fetchall()
        self.assertEqual(ids, [("sec:xnas:aapl", "Apple Inc."), ("sec:xnys:msft", "Microsoft Corp")])

        cur = self.store.connection.execute(
            "SELECT provider, provider_entity_id, entity_id, active_from, metadata FROM provider_entity_map ORDER BY provider_entity_id;"
        )
        maps = cur.fetchall()
        self.assertEqual(
            maps,
            [
                ("provider:financedatabase", "AAPL", "sec:xnas:aapl", None, '{"isin": "US0378331005"}'),
                ("provider:financedatabase", "MSFT", "sec:xnys:msft", None, '{"isin": "US5949181045"}'),
            ],
        )


if __name__ == "__main__":
    unittest.main()
