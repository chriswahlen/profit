from __future__ import annotations

import os
import tempfile
import unittest

from config import Config
from data_sources.entity import EntityStore, EntityType
from scripts.seed_exchanges import seed_exchanges, EXCHANGES


class SeedExchangesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = os.path.join(self.tmpdir.name, "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_exchanges_inserts_and_maps_fd_codes(self):
        inserted, skipped = seed_exchanges(EXCHANGES[:3], self.store)  # take a small subset
        self.assertEqual(inserted, 3)
        self.assertEqual(skipped, 0)

        cur = self.store.connection.execute(
            "SELECT entity_id, entity_type, name FROM entities WHERE entity_id LIKE 'mic:%' ORDER BY entity_id"
        )
        rows = cur.fetchall()
        ids = {r[0] for r in rows}
        self.assertSetEqual(ids, {"mic:xnas", "mic:xnys", "mic:xase"})

        cur = self.store.connection.execute(
            "SELECT provider, provider_entity_id, entity_id FROM provider_entity_map ORDER BY provider_entity_id"
        )
        mappings = cur.fetchall()
        # subset codes from the three exchanges
        expected_codes = set(["NMS", "NGM", "NCM", "NAS", "XNAS", "ASE", "XASE", "NYQ", "NYS", "XNYS"])
        mapped_codes = {row[1] for row in mappings}
        self.assertTrue(expected_codes.issubset(mapped_codes))

        cur = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id, relation FROM entity_entity_map ORDER BY relation"
        )
        relations = cur.fetchall()
        self.assertTrue(any(rel[2] == "traded_in" for rel in relations))


if __name__ == "__main__":
    unittest.main()
