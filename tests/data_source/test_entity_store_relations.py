from __future__ import annotations

import os
import tempfile
import unittest

from config import Config
from data_sources.entity import Entity, EntityStore, EntityType


class EntityStoreRelationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = os.path.join(self.tmpdir.name, "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_entity_exists_and_map_relation(self):
        a = Entity(entity_id="sec:xnas:aapl", entity_type=EntityType.SECURITY, name="AAPL")
        b = Entity(entity_id="company:us:apple-inc", entity_type=EntityType.COMPANY, name="Apple Inc")
        self.store.upsert_entity(a)
        self.store.upsert_entity(b)

        self.assertTrue(self.store.entity_exists(a.entity_id))
        self.assertFalse(self.store.entity_exists("sec:xnas:msft"))

        res = self.store.map_entity_relation(src_entity_id=b.entity_id, dst_entity_id=a.entity_id, relation="lists_on")
        self.assertEqual(res.updated, 1)

        cur = self.store.connection.execute(
            "SELECT src_entity_id, dst_entity_id, relation FROM entity_entity_map;"
        )
        self.assertEqual(cur.fetchall(), [(b.entity_id, a.entity_id, "lists_on")])


if __name__ == "__main__":
    unittest.main()
