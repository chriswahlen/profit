from __future__ import annotations

from pathlib import Path

from profit.agent.retrievers.snippet import SnippetRetriever
from profit.agent.snippets import SnippetStore


def test_store_and_lookup_snippets(tmp_path: Path) -> None:
    store = SnippetStore(path=tmp_path / "snippets.sqlite")
    retriever = SnippetRetriever(store=store)
    snippet = {
        "title": "Insight",
        "body": ["Point 1"],
        "tags": ["insight"],
        "related_instruments": ["XNAS|AAPL"],
        "source_provider": "agent",
    }
    stored = retriever.fetch({"action": "store", "snippet": snippet})
    assert stored.payload["snippet"]["title"] == "Insight"

    lookup = retriever.fetch({"action": "lookup", "filters": {"tags": ["insight"]}, "limit": 1})
    assert lookup.snippet_summaries
