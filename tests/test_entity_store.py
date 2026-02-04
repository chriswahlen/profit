from datetime import datetime, timedelta, timezone

import pytest

from profit.catalog.entity_store import EntityStore, validate_entity_id
from profit.catalog.types import (
    EntityIdentifierRecord,
    EntityRecord,
)


def test_validate_entity_id_lowercase_only():
    valid = "company:nasdaq:aapl"
    validate_entity_id(valid)  # should not raise

    with pytest.raises(ValueError):
        validate_entity_id("Company:NASDAQ:AAPL")


def test_identifier_upsert_updates_last_seen(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    store.upsert_entities([EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc.")])
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])

    first_seen = datetime(2024, 1, 1, tzinfo=timezone.utc)
    second_seen = first_seen + timedelta(days=1)

    rec = EntityIdentifierRecord(
        entity_id="company:nasdaq:aapl",
        scheme="provider:sec:edgar",
        value="0000320193",
        provider_id="sec:edgar",
        active_from=None,
        active_to=None,
        last_seen=first_seen,
    )
    store.upsert_identifiers([rec])

    # re-upsert with later last_seen should update last_seen
    rec_later = rec.__class__(**{**rec.__dict__, "last_seen": second_seen})
    store.upsert_identifiers([rec_later])

    row = store.conn.execute(
        "SELECT last_seen FROM entity_identifier WHERE entity_id=? AND scheme=? ORDER BY last_seen DESC LIMIT 1",
        (rec.entity_id, rec.scheme),
    ).fetchone()
    assert row is not None
    assert row["last_seen"].startswith(second_seen.isoformat()[:19])


def test_resolve_entity_id_uses_identifiers(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    store.upsert_entities(
        [
            EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc."),
        ]
    )
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    store.upsert_identifiers(
        [
            EntityIdentifierRecord(
                entity_id="company:nasdaq:aapl",
                scheme="ticker",
                value="AAPL",
                provider_id="sec:edgar",
                last_seen=datetime(2025, 1, 1, tzinfo=timezone.utc),
            ),
        ]
    )
    assert store.resolve_entity_id("company:nasdaq:aapl") == "company:nasdaq:aapl"
    assert store.resolve_entity_id("aapl") == "company:nasdaq:aapl"
    assert store.resolve_entity_id("AaPl") == "company:nasdaq:aapl"


def test_resolve_identifier_supports_scheme_and_provider(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    store.upsert_entities(
        [
            EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc."),
        ]
    )
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    store.upsert_identifiers(
        [
            EntityIdentifierRecord(
                entity_id="company:nasdaq:aapl",
                scheme="sec:cik",
                value="0000320193",
                provider_id="sec:edgar",
                last_seen=datetime(2025, 2, 1, tzinfo=timezone.utc),
            ),
        ]
    )

    assert store.resolve_identifier("company:nasdaq:aapl", "sec:cik") == "0000320193"
    assert store.resolve_identifier("company:nasdaq:aapl", "sec:cik", provider_id="sec:edgar") == "0000320193"

