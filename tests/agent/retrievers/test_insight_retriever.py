from __future__ import annotations

from pathlib import Path

from profit.agent.retrievers.insight import InsightRetriever
from profit.agent.insights import InsightStore


def test_store_and_lookup_insights(tmp_path: Path) -> None:
    store = InsightStore(path=tmp_path / "insights.sqlite")
    retriever = InsightRetriever(store=store)
    insight = {
        "title": "Insight",
        "body": ["Point 1"],
        "tags": ["insight"],
        "related_instruments": ["XNAS|AAPL"],
        "source_provider": "agent",
    }
    stored = retriever.fetch({"action": "store", "insight": insight})
    assert stored.payload["insight"]["title"] == "Insight"

    lookup = retriever.fetch({"action": "lookup", "filters": {"tags": ["insight"]}, "limit": 1})
    assert lookup.insight_summaries


def test_normalization_notes_reported_for_duplicate_tags(tmp_path: Path) -> None:
    store = InsightStore(path=tmp_path / "insights.sqlite")
    retriever = InsightRetriever(store=store)
    insight = {
        "title": "Insight",
        "body": ["Point 1"],
        "tags": ["insight", "insight"],
        "related_instruments": ["XNAS|AAPL"],
    }
    response = retriever.fetch({"action": "store", "insight": insight})
    assert "normalization_notes" in response.payload
    assert "tags deduplicated" in response.payload["normalization_notes"]
