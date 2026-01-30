from __future__ import annotations

import json
from datetime import datetime, timezone

from profit.cache import FileCache
from profit.catalog import EntityStore
from profit.catalog.seeders import SecCompanyTickerSeeder


def _mock_fetch(url: str, *, timeout: float, headers=None):
    sample = {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corporation"},
    }
    return type("Resp", (), {"status": 200, "body": json.dumps(sample).encode(), "headers": {}})()


def test_sec_seeder_writes_entities_and_identifiers(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    cache = FileCache(base_dir=tmp_path / "cache")

    # Provide required SEC user agent env var
    import os
    os.environ["PROFIT_SEC_USER_AGENT"] = "profit-tests/1.0 contact:test@example.com"

    seeder = SecCompanyTickerSeeder(cache=cache, allow_network=False, fetch_fn=_mock_fetch)
    result = seeder.seed(store)

    assert result.entities_written == 2
    assert result.identifiers_written == 4

    row = store.conn.execute("SELECT name, country_iso2 FROM entity WHERE entity_id='cik:0000320193'").fetchone()
    assert row["name"] == "Apple Inc."
    assert row["country_iso2"] == "US"

    id_row = store.conn.execute(
        "SELECT scheme, value, provider_id FROM entity_identifier WHERE entity_id='cik:0000320193' ORDER BY scheme"
    ).fetchall()
    schemes = {(r["scheme"], r["value"], r["provider_id"]) for r in id_row}
    assert ("sec:cik", "0000320193", "sec:edgar") in schemes
    assert ("ticker:us", "AAPL", "sec:edgar") in schemes

    # Tombstone check: remove one from current set and rerun
    def _mock_fetch_drop(url: str, *, timeout: float, headers=None):
        sample = {
            "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corporation"},
        }
        return type("Resp", (), {"status": 200, "body": json.dumps(sample).encode(), "headers": {}})()

    seeder2 = SecCompanyTickerSeeder(cache=cache, allow_network=False, fetch_fn=_mock_fetch_drop)
    seeder2.seed(store)

    row = store.conn.execute(
        "SELECT active_to FROM entity_identifier WHERE entity_id='cik:0000320193' AND scheme='ticker:us' AND value='AAPL'"
    ).fetchone()
    assert row["active_to"] is not None
