from __future__ import annotations

from typing import Iterable, List, Dict, Any


class InsightStore:
    """Minimal stub insight store for agent scaffolding."""

    def lookup_insights(self, *, tags: Iterable[str], active_at: str, limit: int = 5) -> List[Dict[str, Any]]:
        return []

    def store_insight(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return payload

