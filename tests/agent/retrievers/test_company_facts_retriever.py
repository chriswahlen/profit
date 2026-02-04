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
from profit.config import ProfitConfig


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
    assert result.data_needs[0]["error_code"] == "entity_not_found"


def test_filters_facts_by_lowercase_filings(tmp_path: Path) -> None:
    store = EntityStore(tmp_path / "entity.sqlite", readonly=False)
    store.upsert_providers([("test", "Test Provider", None)])
    entity = EntityRecord(entity_id="xnas_combo", entity_type="company", name="Combo Co")
    store.upsert_entities([entity])
    identifier = EntityIdentifierRecord(
        entity_id="xnas_combo",
        scheme="ticker",
        value="XNAS|COMBO",
        provider_id="test",
        last_seen=datetime.now(timezone.utc),
    )
    store.upsert_identifiers([identifier], default_last_seen=datetime.now(timezone.utc))
    store.upsert_finance_facts(
        [
            FinanceFactRecord(
                entity_id="xnas_combo",
                provider_id="test",
                provider_entity_id="XNAS|COMBO",
                record_id="rec-1",
                report_id="10-Q",
                report_key="Revenues",
                period_end=datetime(2024, 12, 31, tzinfo=timezone.utc),
                units="USD",
                value=1.0,
                asof=datetime.now(timezone.utc),
            ),
            FinanceFactRecord(
                entity_id="xnas_combo",
                provider_id="test",
                provider_entity_id="XNAS|COMBO",
                record_id="rec-2",
                report_id="10-K",
                report_key="Revenues",
                period_end=datetime(2024, 12, 31, tzinfo=timezone.utc),
                units="USD",
                value=2.0,
                asof=datetime.now(timezone.utc),
            ),
        ]
    )

    retriever = CompanyFactsRetriever(store=store)
    request = {
        "companies": ["XNAS|COMBO"],
        "filings": ["10-q"],
        "fields": [{"key": "Revenues"}],
    }
    result = retriever.fetch(request)
    facts = result.payload["data"][0]["facts"][0]["facts"]
    assert len(facts) == 1
    assert facts[0]["report_id"].lower().startswith("10-q")


class RecordingCatchupRunner:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def run(self, cik: str, cfg: ProfitConfig) -> None:
        self.calls.append(cik)


class FailingCatchupRunner:
    def run(self, cik: str, cfg: ProfitConfig) -> None:
        raise RuntimeError("boom")


def _build_profit_config(tmp_path: Path) -> ProfitConfig:
    return ProfitConfig(
        data_root=tmp_path,
        cache_root=tmp_path / "cache",
        store_path=tmp_path / "profit.sqlite",
        log_level="INFO",
        refresh_catalog=False,
    )


def test_runs_edgar_catchup_runner(tmp_path: Path) -> None:
    store = EntityStore(tmp_path / "entity.sqlite", readonly=False)
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    entity = EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc.")
    store.upsert_entities([entity])
    store.upsert_identifiers(
        [
            EntityIdentifierRecord(
                entity_id="company:nasdaq:aapl",
                scheme="ticker:us",
                value="XNAS|AAPL",
                provider_id="sec:edgar",
                last_seen=datetime.now(timezone.utc),
            ),
            EntityIdentifierRecord(
                entity_id="company:nasdaq:aapl",
                scheme="sec:cik",
                value="0000320193",
                provider_id="sec:edgar",
                last_seen=datetime.now(timezone.utc),
            ),
        ],
        default_last_seen=datetime.now(timezone.utc),
    )

    runner = RecordingCatchupRunner()
    retriever = CompanyFactsRetriever(
        store=store,
        catchup_runner=runner,
        catchup_config=_build_profit_config(tmp_path),
    )
    request = {
        "companies": ["XNAS|AAPL"],
        "filings": ["10-K"],
        "fields": [{"key": "Revenues"}],
    }
    retriever.fetch(request)
    assert runner.calls == ["0000320193"]


def test_catchup_failure_reports_data_need(tmp_path: Path) -> None:
    store = EntityStore(tmp_path / "entity.sqlite", readonly=False)
    store.upsert_providers([("sec:edgar", "SEC EDGAR", None)])
    store.upsert_entities([EntityRecord(entity_id="company:nasdaq:aapl", entity_type="company", name="Apple Inc.")])
    store.upsert_identifiers(
        [
            EntityIdentifierRecord(
                entity_id="company:nasdaq:aapl",
                scheme="ticker:us",
                value="XNAS|AAPL",
                provider_id="sec:edgar",
                last_seen=datetime.now(timezone.utc),
            ),
            EntityIdentifierRecord(
                entity_id="company:nasdaq:aapl",
                scheme="sec:cik",
                value="0000320193",
                provider_id="sec:edgar",
                last_seen=datetime.now(timezone.utc),
            ),
        ],
        default_last_seen=datetime.now(timezone.utc),
    )

    retriever = CompanyFactsRetriever(
        store=store,
        catchup_runner=FailingCatchupRunner(),
        catchup_config=_build_profit_config(tmp_path),
    )
    request = {
        "companies": ["XNAS|AAPL"],
        "filings": ["10-K"],
        "fields": [{"key": "Revenues"}],
    }
    result = retriever.fetch(request)
    assert any(need["error_code"] == "edgar_catchup_failed" for need in result.data_needs)


def test_resolves_entity_from_canonical_ticker(tmp_path: Path) -> None:
    store = EntityStore(tmp_path / "entity.sqlite", readonly=False)
    store.upsert_providers(
        [
            ("test", "Test Provider", None),
            ("sec:edgar", "SEC EDGAR", None),
        ]
    )
    entity = EntityRecord(entity_id="company:nasdaq:googl", entity_type="company", name="Alphabet")
    store.upsert_entities([entity])
    store.upsert_identifiers(
        [
            EntityIdentifierRecord(
                entity_id="company:nasdaq:googl",
                scheme="ticker",
                value="GOOGL",
                provider_id="test",
                last_seen=datetime.now(timezone.utc),
            ),
            EntityIdentifierRecord(
                entity_id="company:nasdaq:googl",
                scheme="sec:cik",
                value="0001652044",
                provider_id="sec:edgar",
                last_seen=datetime.now(timezone.utc),
            ),
        ],
        default_last_seen=datetime.now(timezone.utc),
    )
    fact = FinanceFactRecord(
        entity_id="company:nasdaq:googl",
        provider_id="test",
        provider_entity_id="GOOGL",
        record_id="rec-1",
        report_id="10-K",
        report_key="Revenues",
        period_end=datetime(2025, 12, 31, tzinfo=timezone.utc),
        units="USD",
        value=100.0,
        asof=datetime.now(timezone.utc),
    )
    store.upsert_finance_facts([fact])

    retriever = CompanyFactsRetriever(
        store=store,
        catchup_runner=RecordingCatchupRunner(),
        catchup_config=_build_profit_config(tmp_path),
    )
    request = {
        "companies": ["XNAS|GOOGL"],
        "filings": ["10-K"],
        "fields": [{"key": "Revenues"}],
    }
    result = retriever.fetch(request)
    assert result.payload["data"]
