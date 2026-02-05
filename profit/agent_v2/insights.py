from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Iterable

from profit.agent.snippets import SnippetStore
from profit.agent.types import SnippetSummary
from profit.agent_v2.models import SnippetWriteback


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class InsightLookup:
    tags: list[str]
    freshness_horizon_days: int


class InsightsManager:
    def __init__(self, store: SnippetStore | None = None) -> None:
        self.store = store or SnippetStore()

    def lookup(self, lookups: Iterable[InsightLookup], *, limit: int = 5) -> list[SnippetSummary]:
        results: list[SnippetSummary] = []
        seen_ids: set[str] = set()
        now = datetime.now(timezone.utc)
        for lookup in lookups:
            active_at = (now - timedelta(days=lookup.freshness_horizon_days)).isoformat()
            hits = self.store.lookup(tags=lookup.tags, active_at=active_at, limit=limit)
            for hit in hits:
                snippet_id = hit.get("snippet_id", "")
                if not snippet_id or snippet_id in seen_ids:
                    continue
                seen_ids.add(snippet_id)
                results.append(
                    SnippetSummary(
                        snippet_id=snippet_id,
                        title=hit.get("title", ""),
                        body=list(hit.get("body") or []),
                        created_at=hit.get("created_at", ""),
                        matched_tags=lookup.tags,
                    )
                )
        return results

    def store(self, snippets: Iterable[SnippetWriteback]) -> list[dict]:
        stored: list[dict] = []
        for snippet in snippets:
            payload = {
                "title": snippet.title,
                "body": list(snippet.body),
                "tags": list(snippet.tags),
                "related_instruments": list(snippet.related_instruments or []),
                "related_regions": list(snippet.related_regions or []),
                "source_provider": "agent_v2",
                "created_at": _now_iso(),
                "expires_at": snippet.expires_at_utc,
            }
            stored.append(self.store.store_snippet(payload))
        return stored

