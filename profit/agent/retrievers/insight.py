from __future__ import annotations

from profit.agent.retrievers.base import BaseRetriever, RetrieverResult
from profit.agent.insights import InsightStore
from profit.agent.types import InsightSummary


class InsightRetriever(BaseRetriever):
    def __init__(self, store: InsightStore | None = None) -> None:
        self.store = store or InsightStore()

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        action = request.get("action")
        if action == "store":
            insight = request["insight"]
            normalized_insight = self._normalize_insight(insight)
            stored = self.store.store_insight(normalized_insight)
            notes = self._normalization_notes(insight, normalized_insight)
            payload = {
                "type": "insight_store",
                "insight": stored,
            }
            if notes:
                payload["normalization_notes"] = notes
            return RetrieverResult(payload=payload)

        if action == "lookup":
            filters = request.get("filters", {})
            matches = self.store.lookup_insights(
                tags=filters.get("tags"),
                related_instruments=filters.get("related_instruments"),
                active_at=filters.get("active_at"),
                limit=request.get("limit", 5),
            )
            summaries = [
                InsightSummary(
                    insight_id=entry["insight_id"],
                    title=entry["title"],
                    body=entry["body"],
                    created_at=entry["created_at"],
                    matched_tags=list(filters.get("tags") or []),
                )
                for entry in matches
            ]
            return RetrieverResult(
                payload={"type": "insight_lookup", "matches": matches},
                insight_summaries=summaries,
            )

        raise ValueError(f"unknown insight action: {action}")

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
    def _normalize_insight(insight: dict) -> dict:
        normalized = dict(insight)
        tags = insight.get("tags")
        if tags:
            seen: set[str] = set()
            deduped: list[str] = []
            for tag in tags:
                if tag not in seen:
                    deduped.append(tag)
                    seen.add(tag)
            normalized["tags"] = deduped
        return normalized
