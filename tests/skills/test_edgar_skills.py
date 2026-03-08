from __future__ import annotations

import pytest

from agents.financial_advisor.skills.edgar_skills import EdgarSkills, SEC_PROVIDER
from config import Config
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.entity import Entity, EntityStore, EntityType


@pytest.fixture
def edgar_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("PROFIT_DATA_PATH", str(tmp_path))
    cfg = Config()
    store = EdgarDataStore(cfg)
    entity_store = EntityStore(cfg)
    try:
        yield store, entity_store
    finally:
        store.close()
        entity_store.close()


def _seed_context_and_fact(store: EdgarDataStore, cik: str, accession: str, concept: str, value: float) -> None:
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
    concept_id = store.get_or_create_xbrl_concept(concept, label=concept.split(":")[-1], data_type="monetaryItemType")
    store.insert_xbrl_fact(
        accession,
        concept_id,
        context_id,
        value_numeric=value,
        value_raw=str(value),
    )
    conn.commit()


def test_edgar_skill_list_returns_fact_descriptor(edgar_environment):
    store, entity_store = edgar_environment
    skills = EdgarSkills(store, entity_store)
    descriptors = skills.list_skills()
    assert any(desc.skill_id == EdgarSkills.SKILL_FACTS for desc in descriptors)


def test_edgar_prompt_mentions_concepts(edgar_environment):
    store, entity_store = edgar_environment
    prompt = EdgarSkills(store, entity_store).describe_skill_usage(EdgarSkills.SKILL_FACTS)
    assert "concepts" in prompt.prompt
    assert "Example input" in prompt.prompt


def test_execute_facts_by_cik_returns_rows(edgar_environment):
    store, entity_store = edgar_environment
    cik = "0000320193"
    accession = "0000320193-24-000001"
    _seed_context_and_fact(store, cik, accession, "us-gaap:Assets", 200000.0)

    skills = EdgarSkills(store, entity_store)
    payload = {
        "cik": cik,
        "concepts": ["us-gaap:Assets"],
    }
    result = skills.execute_skill(EdgarSkills.SKILL_FACTS, payload)
    assert result.metadata["row_count"] == 1
    record = result.records[0]
    assert record["concept"] == "us-gaap:Assets"
    assert record["accession"] == accession


def test_execute_facts_accepts_symbol_mapping(edgar_environment):
    store, entity_store = edgar_environment
    cik = "0000320193"
    accession = "0000320193-24-000002"
    _seed_context_and_fact(store, cik, accession, "us-gaap:Liabilities", 150000.0)

    entity = Entity(entity_id="company:us:apple-inc", entity_type=EntityType.COMPANY, name="Apple Inc.")
    entity_store.upsert_entity(entity)
    entity_store.upsert_provider(SEC_PROVIDER, description="SEC EDGAR")
    entity_store.map_provider_entity(
        provider=SEC_PROVIDER,
        provider_entity_id=cik,
        entity_id=entity.entity_id,
    )

    skills = EdgarSkills(store, entity_store)
    payload = {
        "symbol": entity.entity_id,
        "concepts": ["us-gaap:Liabilities"],
    }
    result = skills.execute_skill(EdgarSkills.SKILL_FACTS, payload)
    assert result.metadata["cik"] == cik
    assert result.records[0]["concept"] == "us-gaap:Liabilities"


def test_execute_facts_missing_concept_list_raises(edgar_environment):
    store, entity_store = edgar_environment
    skills = EdgarSkills(store, entity_store)
    with pytest.raises(ValueError):
        skills.execute_skill(EdgarSkills.SKILL_FACTS, {"cik": "0000320193"})
