from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Iterable

from profit.agent.insights import InsightStore
from profit.agent.types import InsightSummary
from profit.agent_v2.models import InsightWriteback


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class InsightLookup:
    tags: list[str]
    freshness_horizon_days: int


class InsightsManager:
    def __init__(self, store: InsightStore | None = None) -> None:
        self.store = store or InsightStore()

    def lookup(self, lookups: Iterable[InsightLookup], *, limit: int = 5) -> list[InsightSummary]:
        results: list[InsightSummary] = []
        seen_ids: set[str] = set()
        now = datetime.now(timezone.utc)
        for lookup in lookups:
            active_at = (now - timedelta(days=lookup.freshness_horizon_days)).isoformat()
            hits = self.store.lookup_insights(tags=lookup.tags, active_at=active_at, limit=limit)
            for hit in hits:
                insight_id = hit.get("insight_id", "")
                if not insight_id or insight_id in seen_ids:
                    continue
                seen_ids.add(insight_id)
                results.append(
                    InsightSummary(
                        insight_id=insight_id,
                        title=hit.get("title", ""),
                        body=list(hit.get("body") or []),
                        created_at=hit.get("created_at", ""),
                        matched_tags=lookup.tags,
                    )
                )
        return results

    def store(self, insights: Iterable[InsightWriteback]) -> list[dict]:
        stored: list[dict] = []
        for insight in insights:
            payload = {
                "title": insight.title,
                "body": list(insight.body),
                "tags": list(insight.tags),
                "related_instruments": list(insight.related_instruments or []),
                "related_regions": list(insight.related_regions or []),
                "source_provider": "agent_v2",
                "created_at": _now_iso(),
                "expires_at": insight.expires_at_utc,
            }
            stored.append(self.store.store_insight(payload))
        return stored
