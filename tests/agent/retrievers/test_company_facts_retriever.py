from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from profit.agent.retrievers.company_facts import CompanyFactsRetriever
from profit.catalog.entity_store import EntityStore
from profit.catalog.types import (
    EntityIdentifierRecord,
    EntityRecord,
    FinanceFactRecord,
)


def test_serves_known_entity_and_fields(tmp_path: Path) -> None:
    store = EntityStore(tmp_path / "entity.sqlite", readonly=False)
    store.upsert_providers([("test", "Test Provider", None)])
    entity = EntityRecord(entity_id="xnas_test", entity_type="company", name="Test Co")
    store.upsert_entities([entity])
    identifier = EntityIdentifierRecord(
        entity_id="xnas_test",
        scheme="ticker",
        value="XNAS|TEST",
        provider_id="test",
        last_seen=datetime.now(timezone.utc),
    )
    store.upsert_identifiers([identifier], default_last_seen=datetime.now(timezone.utc))
    fact = FinanceFactRecord(
        entity_id="xnas_test",
        provider_id="test",
        provider_entity_id="XNAS|TEST",
        record_id="rec-1",
        report_id="10-K",
        report_key="Revenues",
        period_end=datetime(2024, 12, 31, tzinfo=timezone.utc),
        units="USD",
        value=123456.0,
        asof=datetime.now(timezone.utc),
    )
    store.upsert_finance_facts([fact])

    retriever = CompanyFactsRetriever(store=store)
    request = {
        "companies": ["XNAS|TEST"],
        "filings": ["10-K"],
        "fields": [{"key": "Revenues"}],
    }
    result = retriever.fetch(request)
    assert result.payload["data"]
    assert result.payload["data"][0]["facts"][0]["facts"][0]["value"] == pytest.approx(123456.0)


def test_reports_missing_company(tmp_path: Path) -> None:
    store = EntityStore(tmp_path / "entity.sqlite", readonly=False)
    retriever = CompanyFactsRetriever(store=store)
    request = {
        "companies": ["UNKNOWN"],
        "filings": ["10-K"],
        "fields": [{"key": "Revenues"}],
    }
    result = retriever.fetch(request)
    assert result.data_needs
