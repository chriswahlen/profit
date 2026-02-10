from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional, Sequence

from agent_v2.models import Insight


@dataclass(frozen=True)
class InsightRow:
    insight_id: int
    insight: Insight


class InsightsStore:
    """
    Small sqlite-backed store for cross-run insights.

    Uses the same sqlite database file as the agent snapshot store by default, but stores
    records in its own tables.
    """

    def __init__(self, *, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_v2_insights (
              insight_id INTEGER PRIMARY KEY AUTOINCREMENT,
              text TEXT NOT NULL,
              tags_json TEXT NOT NULL,
              start_date TEXT,
              end_date TEXT,
              created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_agent_v2_insights_dates
            ON agent_v2_insights (start_date, end_date)
            """
        )
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def add(self, insights: Iterable[Insight]) -> None:
        if self._conn is None:
            raise RuntimeError("InsightsStore is not open")
        rows = []
        for ins in insights:
            rows.append(
                (
                    ins.text,
                    json.dumps(list(ins.tags), sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                    ins.start_date.isoformat() if ins.start_date else None,
                    ins.end_date.isoformat() if ins.end_date else None,
                )
            )
        if not rows:
            return
        with self._conn:
            self._conn.executemany(
                """
                INSERT INTO agent_v2_insights (text, tags_json, start_date, end_date)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )

    def search(
        self,
        *,
        tags: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
        limit: int = 25,
    ) -> list[InsightRow]:
        if self._conn is None:
            raise RuntimeError("InsightsStore is not open")

        normalized_tags = [t.strip().lower() for t in tags if t.strip()]
        if not normalized_tags:
            return []

        # Simple scan + filter: small table expected.
        rows = self._conn.execute(
            """
            SELECT insight_id, text, tags_json, start_date, end_date
            FROM agent_v2_insights
            ORDER BY insight_id DESC
            LIMIT ?
            """,
            (int(limit) * 10,),
        ).fetchall()

        out: list[InsightRow] = []
        for insight_id, text, tags_json, s, e in rows:
            try:
                row_tags = [str(x).strip().lower() for x in json.loads(tags_json or "[]")]
            except Exception:
                row_tags = []
            if not set(normalized_tags).intersection(row_tags):
                continue

            row_start = date.fromisoformat(s) if s else None
            row_end = date.fromisoformat(e) if e else None
            if start_date or end_date:
                if row_end and start_date and row_end < start_date:
                    continue
                if row_start and end_date and row_start > end_date:
                    continue

            insight = Insight(
                text=str(text),
                tags=tuple(sorted({t for t in row_tags if t})),
                start_date=row_start,
                end_date=row_end,
            )
            out.append(InsightRow(insight_id=int(insight_id), insight=insight))
            if len(out) >= limit:
                break
        return out

