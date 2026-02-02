from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from profit.agent.types import SnippetSummary

logger = logging.getLogger(__name__)


def _to_iso(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    return ts.astimezone(timezone.utc).isoformat()


class SnippetStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = (path or Path("agent_snippets.sqlite")).resolve()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snippets (
                    snippet_id TEXT PRIMARY KEY,
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snippets_created_at ON snippets(created_at)")
            conn.commit()

    def _normalize(self, value: Iterable[str] | None) -> str:
        return json.dumps(list(value)) if value else "[]"

    def store_snippet(self, snippet: dict) -> dict:
        snippet_id = snippet.get("snippet_id") or f"snippet-{uuid4().hex}"
        created = snippet.get("created_at") or _to_iso(datetime.now(timezone.utc))
        expires = snippet.get("expires_at")
        body = snippet["body"]
        tags = snippet["tags"]
        instruments = snippet.get("related_instruments") or []
        regions = snippet.get("related_regions") or []
        provider = snippet.get("source_provider", "agent")

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO snippets (
                    snippet_id, title, body, tags, related_instruments,
                    related_regions, source_provider, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snippet_id,
                    snippet["title"],
                    json.dumps(body),
                    json.dumps(tags),
                    json.dumps(instruments),
                    json.dumps(regions),
                    provider,
                    created,
                    expires,
                ),
            )
            conn.commit()

        normalized = {
            "snippet_id": snippet_id,
            "title": snippet["title"],
            "body": body,
            "tags": tags,
            "related_instruments": instruments,
            "related_regions": regions,
            "source_provider": provider,
            "created_at": created,
            "expires_at": expires,
        }
        return normalized

    def lookup(
        self,
        *,
        tags: Iterable[str] | None = None,
        related_instruments: Iterable[str] | None = None,
        active_at: str | None = None,
        limit: int = 5,
    ) -> list[dict]:
        with self._connect() as conn:
            cursor = conn.execute("SELECT * FROM snippets ORDER BY created_at DESC")
            rows = cursor.fetchall()

        results: list[dict] = []
        now = datetime.now(timezone.utc)
        for row in rows:
            record = dict(row)
            record_body = json.loads(record["body"])
            record_tags = json.loads(record["tags"])
            record_instruments = json.loads(record["related_instruments"] or "[]")
            record_regions = json.loads(record["related_regions"] or "[]")

            expires = record["expires_at"]
            if expires:
                try:
                    expires_ts = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                except ValueError:
                    expires_ts = None
                if expires_ts and now > expires_ts:
                    continue

            if tags and not any(tag in record_tags for tag in tags):
                continue
            if related_instruments and not any(instr in record_instruments for instr in related_instruments):
                continue
            if active_at:
                try:
                    active_ts = datetime.fromisoformat(active_at.replace("Z", "+00:00"))
                    created_ts = datetime.fromisoformat(record["created_at"].replace("Z", "+00:00"))
                except ValueError:
                    pass
                else:
                    if created_ts > active_ts:
                        continue

            record["body"] = record_body
            record["tags"] = record_tags
            record["related_instruments"] = record_instruments
            record["related_regions"] = record_regions
            results.append(record)
            if len(results) >= limit:
                break

        return results
