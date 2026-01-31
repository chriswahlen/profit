from __future__ import annotations

import json

from profit.cache import FileCache
from profit.catalog import EntityStore
from profit.catalog.seeders import OpenExchangeRatesCurrencySeeder


def _mock_fetch(url: str, *, timeout: float, headers=None):
    sample = {"USD": "United States Dollar", "EUR": "Euro", "JPY": "Japanese Yen"}
    return type("Resp", (), {"status": 200, "body": json.dumps(sample).encode(), "headers": {}})()


def test_oxr_seeder_writes_currencies(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    cache = FileCache(base_dir=tmp_path / "cache")

    seeder = OpenExchangeRatesCurrencySeeder(cache=cache, allow_network=False, fetch_fn=_mock_fetch)
    result = seeder.seed(store)

    assert result.entities_written == 3
    assert result.identifiers_written == 3

    row = store.conn.execute("SELECT name FROM entity WHERE entity_id='ccy:usd'").fetchone()
    assert row["name"] == "United States Dollar"

    ids = store.conn.execute(
        "SELECT value, scheme FROM entity_identifier WHERE entity_id='ccy:usd'"
    ).fetchall()
    assert ("USD", "iso:ccy") in {(r["value"], r["scheme"]) for r in ids}
