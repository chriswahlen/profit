from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from profit.agent_v2.models import EdgarRequest

logger = logging.getLogger(__name__)


def _open_db(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _build_query(params: EdgarRequest["params"]) -> str:
    aliases = ",".join(f"'{alias}'" for alias in params.concept_aliases)
    return (
        "SELECT f.accession, c.qname, ctx.start_date, ctx.end_date, f.value_numeric, f.value_raw, "
        "f.unit_id, f.is_nil "
        "FROM xbrl_fact f "
        "JOIN xbrl_concept c ON f.concept_id=c.concept_id "
        "JOIN xbrl_context ctx ON f.context_id=ctx.context_id "
        "JOIN edgar_accession a ON ctx.accession=a.accession "
        f"WHERE a.cik=? AND c.qname IN ({aliases}) "
        "AND ctx.period_type=? "
        "AND ("
        "(ctx.start_date BETWEEN ? AND ?) "
        "OR (ctx.end_date BETWEEN ? AND ?) "
        "OR (ctx.instant_date BETWEEN ? AND ?)"
        ") "
        "LIMIT ?"
    )


@dataclass(frozen=True)
class EdgarResult:
    payload: dict
    data_needs: list[dict]


class EdgarRetrieverV2:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or Path("data/edgar.sqlite3")

    def fetch(self, request: EdgarRequest) -> EdgarResult:
        params = request.params
        if not self.db_path.exists():
            return EdgarResult(
                payload={"type": "edgar", "request_id": request.request_id, "error": "db missing"},
                data_needs=[{"name": "edgar_db", "reason": "missing file", "criticality": "high"}],
            )
        sql = _build_query(params)
        start_time = time.monotonic()
        with _open_db(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                sql,
                (
                    params.cik,
                    params.period_type,
                    params.start_utc,
                    params.end_utc,
                    params.start_utc,
                    params.end_utc,
                    params.start_utc,
                    params.end_utc,
                    params.limit,
                ),
            )
            rows = [dict(row) for row in cur.fetchall()]
        elapsed = int((time.monotonic() - start_time) * 1000)
        payload = {
            "type": "edgar",
            "request_id": request.request_id,
            "rows": rows,
            "meta": {
                "cik": params.cik,
                "concept_aliases": params.concept_aliases,
                "elapsed_ms": elapsed,
            },
        }
        logger.info("edgar_v2 fetched %s rows elapsed=%sms", len(rows), elapsed)
        return EdgarResult(payload=payload, data_needs=[])
