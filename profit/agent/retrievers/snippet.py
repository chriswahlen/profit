from __future__ import annotations

import logging

from profit.agent.retrievers.base import BaseRetriever, RetrieverResult
from profit.agent.snippets import SnippetStore
from profit.agent.types import SnippetSummary

logger = logging.getLogger(__name__)


class SnippetRetriever(BaseRetriever):
    def __init__(self, store: SnippetStore | None = None) -> None:
        self.store = store or SnippetStore()

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        action = request.get("action")
        if action == "store":
            snippet = request["snippet"]
            stored = self.store.store_snippet(snippet)
            return RetrieverResult(payload={"type": "snippet_store", "snippet": stored})

        if action == "lookup":
            filters = request.get("filters", {})
            matches = self.store.lookup(
                tags=filters.get("tags"),
                related_instruments=filters.get("related_instruments"),
                active_at=filters.get("active_at"),
                limit=request.get("limit", 5),
            )
            summaries = [
                SnippetSummary(
                    snippet_id=entry["snippet_id"],
                    title=entry["title"],
                    body=entry["body"],
                    created_at=entry["created_at"],
                    matched_tags=list(filters.get("tags") or []),
                )
                for entry in matches
            ]
            return RetrieverResult(
                payload={"type": "snippet_lookup", "matches": matches},
                snippet_summaries=summaries,
            )

        raise ValueError(f"unknown snippet action: {action}")
