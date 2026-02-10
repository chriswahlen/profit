from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Dict, Any
from uuid import uuid4

# Default location keeps insights outside git-tracked paths.
DEFAULT_INSIGHTS_PATH = Path("tmp_cache/insights.sqlite")


class InsightStore:
    """SQLite-backed store for agent insights."""

    def __init__(self, *, path: str | Path | None = None) -> None:
        self.path = Path(path) if path else DEFAULT_INSIGHTS_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS insights (
                insight_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                tags TEXT NOT NULL,
                related_instruments TEXT,
                related_regions TEXT,
                source_provider TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT
            )
            """
        )
        self.conn.commit()

    def lookup_insights(
        self,
        *,
        tags: Iterable[str] | None = None,
        related_instruments: Iterable[str] | None = None,
        active_at: str | None = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Return insights that match all requested tags and are active at the given time."""

        cur = self.conn.execute("SELECT insight_id, title, body, tags, related_instruments, related_regions, source_provider, created_at, expires_at FROM insights ORDER BY created_at DESC")
        rows = cur.fetchall()
        active_dt = _parse_dt(active_at)
        tag_set = {t for t in (tags or []) if t}
        inst_set = {i for i in (related_instruments or []) if i}

        matches: List[Dict[str, Any]] = []
        for row in rows:
            insight = {
                "insight_id": row[0],
                "title": row[1],
                "body": json.loads(row[2]),
                "tags": json.loads(row[3]),
                "related_instruments": json.loads(row[4]) if row[4] else [],
                "related_regions": json.loads(row[5]) if row[5] else [],
                "source_provider": row[6],
                "created_at": row[7],
                "expires_at": row[8],
            }

            if tag_set and not tag_set.issubset(set(insight["tags"])):
                continue
            if inst_set and not inst_set.intersection(set(insight.get("related_instruments", []))):
                continue
            if active_dt:
                created_dt = _parse_dt(insight["created_at"])
                expires_dt = _parse_dt(insight.get("expires_at"))
                # Keep only insights that are active on/after active_at. Newer than the freshness window passes.
                if created_dt and created_dt < active_dt:
                    continue
                if expires_dt and expires_dt < active_dt:
                    continue

            matches.append(insight)
            if len(matches) >= limit:
                break

        return matches

    def store_insight(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        insight_id = payload.get("insight_id") or str(uuid4())
        body = payload.get("body") or []
        tags = payload.get("tags") or []
        related_instruments = payload.get("related_instruments") or []
        related_regions = payload.get("related_regions") or []

        normalized = dict(payload)
        normalized.update(
            {
                "insight_id": insight_id,
                "body": body,
                "tags": tags,
                "related_instruments": related_instruments,
                "related_regions": related_regions,
                "created_at": payload.get("created_at") or now,
            }
        )

        self.conn.execute(
            """
            INSERT OR REPLACE INTO insights (
                insight_id, title, body, tags, related_instruments, related_regions, source_provider, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized["insight_id"],
                normalized.get("title", ""),
                json.dumps(list(body)),
                json.dumps(list(tags)),
                json.dumps(list(related_instruments)) if related_instruments else None,
                json.dumps(list(related_regions)) if related_regions else None,
                normalized.get("source_provider"),
                normalized["created_at"],
                normalized.get("expires_at"),
            ),
        )
        self.conn.commit()
        return normalized


def _parse_dt(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None
