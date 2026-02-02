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
            normalized_snippet = self._normalize_snippet(snippet)
            stored = self.store.store_snippet(normalized_snippet)
            notes = self._normalization_notes(snippet, normalized_snippet)
            payload = {
                "type": "snippet_store",
                "snippet": stored,
            }
            if notes:
                payload["normalization_notes"] = notes
            return RetrieverResult(payload=payload)

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

    @staticmethod
    def _normalization_notes(original: dict, stored: dict) -> list[str]:
        notes: list[str] = []
        original_tags = original.get("tags") or []
        stored_tags = stored.get("tags") or []
        if len(original_tags) != len(stored_tags) or set(original_tags) != set(stored_tags):
            if len(set(original_tags)) != len(original_tags):
                notes.append("tags deduplicated")
            else:
                notes.append("tags normalized")
        return notes

    @staticmethod
    def _normalize_snippet(snippet: dict) -> dict:
        normalized = dict(snippet)
        tags = snippet.get("tags")
        if tags:
            seen: set[str] = set()
            deduped: list[str] = []
            for tag in tags:
                if tag not in seen:
                    deduped.append(tag)
                    seen.add(tag)
            normalized["tags"] = deduped
        return normalized
