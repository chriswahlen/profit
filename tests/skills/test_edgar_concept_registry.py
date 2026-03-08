from __future__ import annotations

import pytest

from agents.financial_advisor.skills.edgar_concept_registry import (
    CONCEPT_SEEDS,
    ConceptRegistryBuilder,
)
from config import Config
from data_sources.edgar.edgar_data_store import EdgarDataStore


@pytest.fixture
def edgar_store(tmp_path, monkeypatch):
    monkeypatch.setenv("PROFIT_DATA_PATH", str(tmp_path))
    cfg = Config()
    store = EdgarDataStore(cfg)
    try:
        yield store
    finally:
        store.close()


def _seed_fact(store: EdgarDataStore, cik: str, accession: str, qname: str, value: float) -> None:
    store.record_accession_index(cik, accession, "https://example.com/edgar/", [])
    conn = store.connection
    scheme_id = store.get_or_create_entity_scheme("http://www.sec.gov/CIK")
    cur = conn.execute(
        """
        INSERT INTO xbrl_context (
            accession, context_ref, entity_scheme_id, entity_id,
            period_type, start_date, end_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (accession, "C1", scheme_id, cik, "duration", "2024-01-01", "2024-12-31"),
    )
    context_id = cur.lastrowid
    concept_id = store.get_or_create_xbrl_concept(qname, label=qname.split(":")[-1], data_type="monetaryItemType")
    store.insert_xbrl_fact(
        accession,
        concept_id,
        context_id,
        value_numeric=value,
        value_raw=str(value),
    )
    conn.commit()


def test_registry_groups_concepts_by_cik(edgar_store):
    _seed_fact(edgar_store, "0000320193", "0000320193-24-000001", "us-gaap:Assets", 100.0)
    _seed_fact(edgar_store, "0000320193", "0000320193-24-000002", "dei:EntityLiabilities", 50.0)
    _seed_fact(edgar_store, "0000789019", "0000789019-24-000001", "us-gaap:Revenues", 75.0)

    builder = ConceptRegistryBuilder(edgar_store, seeds=CONCEPT_SEEDS)
    registry = builder.build()

    assets = registry.qnames_for("0000320193", "assets")
    assert "us-gaap:Assets" in assets
    liabilities = registry.qnames_for("0000320193", "liabilities")
    assert "dei:EntityLiabilities" in liabilities
    revenue = registry.qnames_for("0000789019", "revenue")
    assert "us-gaap:Revenues" in revenue


def test_registry_fallback_to_seed_for_missing(edgar_store):
    builder = ConceptRegistryBuilder(edgar_store, seeds=CONCEPT_SEEDS)
    registry = builder.build()
    net_income = registry.qnames_for("0000320193", "net_income")
    assert list(net_income) == list(CONCEPT_SEEDS[3].qnames)  # net_income seed is at index 3


def test_serialize_includes_keys(edgar_store):
    _seed_fact(edgar_store, "0000320193", "0000320193-24-000003", "us-gaap:Assets", 101.0)
    builder = ConceptRegistryBuilder(edgar_store, seeds=CONCEPT_SEEDS)
    registry = builder.build()
    serialized = registry.serialize()
    assert "0000320193" in serialized
    assert "assets" in serialized["0000320193"]
