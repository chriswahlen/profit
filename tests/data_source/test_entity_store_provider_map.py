from __future__ import annotations

import os
import tempfile
import unittest

from config import Config
from data_sources.entity import Entity, EntityStore, EntityType


class EntityStoreProviderMapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = os.path.join(self.tmpdir.name, "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_active_from_can_be_null(self):
        ent = Entity(entity_id="sec:xnas:aapl", entity_type=EntityType.SECURITY, name="AAPL")
        self.store.upsert_entity(ent)
        self.store.upsert_provider("provider:test")
        self.store.map_provider_entity(
            provider="provider:test",
            provider_entity_id="AAPL",
            entity_id=ent.entity_id,
            active_from=None,
            active_to=None,
            metadata=None,
        )
        cur = self.store.connection.execute(
            "SELECT active_from FROM provider_entity_map WHERE provider=? AND provider_entity_id=?",
            ("provider:test", "AAPL"),
        )
        self.assertIsNone(cur.fetchone()[0])


if __name__ == "__main__":
    unittest.main()
