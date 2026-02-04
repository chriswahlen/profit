from datetime import datetime, timedelta, timezone

import pytest

from profit.catalog.entity_store import EntityStore, validate_entity_id
from profit.catalog.types import (
    EntityIdentifierRecord,
    EntityRecord,
    FinanceFactRecord,
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


def test_finance_fact_overwrite_requires_newer_asof(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    store.upsert_entities(
        [EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc.")]
    )

    base_fact = FinanceFactRecord(
        entity_id="company:nasdaq:aapl",
        provider_id="sec:edgar",
        provider_entity_id="0000320193",
        record_id="0001193125-24-000010",
        report_id="10-K-2024",
        report_key="Revenues",
        period_end=datetime(2024, 9, 30, tzinfo=timezone.utc),
        units="currency:USD",
        value=100.0,
        asof=datetime(2024, 11, 1, tzinfo=timezone.utc),
        attrs=None,
    )

    store.upsert_finance_facts([base_fact])

    # newer asof overwrites value
    newer_fact = base_fact.__class__(**{**base_fact.__dict__, "value": 110.0, "asof": base_fact.asof + timedelta(days=1)})
    store.upsert_finance_facts([newer_fact])

    row = store.conn.execute(
        """
        SELECT value, asof FROM company_finance_fact
        WHERE provider_id=? AND provider_entity_id=? AND record_id=? AND report_id=? AND report_key=? AND period_end=?
        """,
        (
            base_fact.provider_id,
            base_fact.provider_entity_id,
            base_fact.record_id,
            base_fact.report_id,
            base_fact.report_key,
            base_fact.period_end.date().isoformat(),
        ),
    ).fetchone()
    assert row["value"] == 110.0

    # older asof conflicting value should raise
    older_fact = base_fact.__class__(**{**base_fact.__dict__, "value": 90.0, "asof": base_fact.asof - timedelta(days=1)})
    with pytest.raises(ValueError):
        store.upsert_finance_facts([older_fact])


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


def test_list_finance_facts_respects_filings(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    store.upsert_entities(
        [EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc.")]
    )

    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    facts = []
    for idx, report_id in enumerate(("10-K-2025", "10-Q-2024"), start=1):
        facts.append(
            FinanceFactRecord(
                entity_id="company:nasdaq:aapl",
                provider_id="sec:edgar",
                provider_entity_id="0000320193",
                record_id=f"rec-{idx}",
                report_id=report_id,
                report_key="Revenues",
                period_end=datetime(2024, 9, 30, tzinfo=timezone.utc),
                units="currency:USD",
                value=100.0 + idx,
                asof=base + timedelta(days=idx),
            )
        )
    store.upsert_finance_facts(facts)

    all_rows = store.list_finance_facts("company:nasdaq:aapl", "Revenues")
    assert len(all_rows) == 2
    filtered = store.list_finance_facts("company:nasdaq:aapl", "Revenues", filings=["10-q"])
    assert len(filtered) == 1
    assert filtered[0]["report_id"].lower().startswith("10-q")


def test_list_finance_facts_default_filings_and_limit(tmp_path):
    store = EntityStore(tmp_path / "entities.sqlite3")
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    store.upsert_entities(
        [EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc.")]
    )

    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    facts = []
    for idx, report_id in enumerate(("10-K-2025", "10-Q-2024", "8-K-2025"), start=1):
        facts.append(
            FinanceFactRecord(
                entity_id="company:nasdaq:aapl",
                provider_id="sec:edgar",
                provider_entity_id="0000320193",
                record_id=f"rec-{idx}",
                report_id=report_id,
                report_key="Revenues",
                period_end=datetime(2024, 9, 30, tzinfo=timezone.utc),
                units="currency:USD",
                value=100.0 + idx,
                asof=base + timedelta(days=idx),
            )
        )
    store.upsert_finance_facts(facts)

    all_rows = store.list_finance_facts("company:nasdaq:aapl", "Revenues")
    assert len(all_rows) == 3
    limited = store.list_finance_facts("company:nasdaq:aapl", "Revenues", limit=2)
    assert len(limited) == 2
