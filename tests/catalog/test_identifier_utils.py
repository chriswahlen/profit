from datetime import datetime, timezone
from pathlib import Path

from profit.catalog.entity_store import EntityStore
from profit.catalog.identifier_utils import resolve_cik_from_identifier, resolve_entity_id_from_identifier
from profit.catalog.types import EntityIdentifierRecord, EntityRecord


def _make_store(tmp_path: Path) -> EntityStore:
    store = EntityStore(tmp_path / "entity.sqlite", readonly=False)
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    entity = EntityRecord(entity_id="company:nasdaq:googl", entity_type="company", name="Alphabet")
    store.upsert_entities([entity])
    store.upsert_identifiers(
        [
            EntityIdentifierRecord(
                entity_id=entity.entity_id,
                scheme="ticker",
                value="GOOGL",
                provider_id="sec:edgar",
                last_seen=datetime.now(timezone.utc),
            ),
            EntityIdentifierRecord(
                entity_id=entity.entity_id,
                scheme="sec:cik",
                value="0001652044",
                provider_id="sec:edgar",
                last_seen=datetime.now(timezone.utc),
            ),
        ],
        default_last_seen=datetime.now(timezone.utc),
    )
    return store


def test_resolve_entity_id_from_canonical_symbol(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        entity_id = resolve_entity_id_from_identifier(store, "XNAS|GOOGL")
    finally:
        store.close()
    assert entity_id == "company:nasdaq:googl"


def test_resolve_cik_from_identifier_falls_back_to_digits(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        cik = resolve_cik_from_identifier(store, "XNAS|GOOGL")
        assert cik == "0001652044"
        assert resolve_cik_from_identifier(store, "1652044") == "0001652044"
        assert resolve_cik_from_identifier(store, "CIK:1652044") == "0001652044"
    finally:
        store.close()
