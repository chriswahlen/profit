from datetime import date, datetime, timezone

from profit.agent import retrievers
from profit.agent.types import RetrievalPlan
from profit.catalog.entity_store import EntityStore
from profit.catalog.types import FinanceFactRecord, EntityRecord, EntityIdentifierRecord
from profit.sources.edgar.sec_edgar import SEC_PROVIDER_ID


def _seed_finance_store(tmp_path):
    store_path = tmp_path / "profit.sqlite"
    store = EntityStore(store_path)
    store.upsert_providers([(SEC_PROVIDER_ID, "SEC EDGAR", "test")])
    entity_id = "us:com:apple"
    store.upsert_entities(
        [
            EntityRecord(
                entity_id=entity_id,
                entity_type="company",
                name="Apple Inc.",
                country_iso2="US",
                status="active",
                attrs={},
            )
        ]
    )
    store.upsert_identifiers(
        [
            EntityIdentifierRecord(
                entity_id=entity_id,
                scheme="sec:cik",
                value="0000320193",
                provider_id=SEC_PROVIDER_ID,
                active_from=None,
                active_to=None,
                last_seen=datetime.now(timezone.utc),
            )
        ]
    )
    fact = FinanceFactRecord(
        entity_id=entity_id,
        provider_id=SEC_PROVIDER_ID,
        provider_entity_id="0000320193",
        record_id="r1",
        report_id="10-K",
        report_key="Revenue",
        period_start=datetime(2023, 9, 24, tzinfo=timezone.utc),
        period_end=datetime(2024, 9, 28, tzinfo=timezone.utc),
        decimals=0,
        dimensions_sig=None,
        is_consolidated=True,
        amendment_flag=False,
        filed_at=datetime(2024, 11, 5, tzinfo=timezone.utc),
        units="USD",
        value=383285000000.0,
        asof=datetime(2024, 11, 6, tzinfo=timezone.utc),
        attrs={},
    )
    store.upsert_finance_facts([fact])
    return store_path


def test_fetch_edgar_includes_finance_facts(tmp_path):
    store_path = _seed_finance_store(tmp_path)
    plan = RetrievalPlan(source="edgar", filings=("0000320193",), start=date(2024, 1, 1), end=date(2024, 12, 31))
    result = retrievers.fetch(plan, entity_store_path=store_path, edgar_docs_path=tmp_path / "edgar")
    payload = result.payload
    entities = payload["facts"]
    assert entities
    facts = entities[0]["facts"]
    assert facts
    assert facts[0]["report_id"] == "10-K"
    assert facts[0]["report_key"] == "Revenue"
    assert facts[0]["units"] == "USD"
    assert facts[0]["period_start"].startswith("2023-09-24")
    assert facts[0]["period_end"].startswith("2024-09-28")
    assert payload["unresolved_filings"] == []
