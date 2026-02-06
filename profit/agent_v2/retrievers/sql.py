from __future__ import annotations

import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from profit.agent_v2.models import SqlRequest

logger = logging.getLogger(__name__)


_SQL_START_RE = re.compile(r"^\s*(with|select)\b", re.IGNORECASE)
_SQL_DISALLOWED_RE = re.compile(
    r"\b(attach|detach|pragma|vacuum|insert|update|delete|drop|alter|create|replace)\b",
    re.IGNORECASE,
)


def _resolve_dataset_path(dataset: str) -> Path:
    if dataset == "edgar":
        return Path("data/edgar.sqlite3")
    if dataset == "real_estate":
        return Path("data/redfin.sqlite")
    raise ValueError(f"unknown dataset: {dataset}")


def _open_sqlite_ro(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _wrap_limit(sql: str, max_rows: int) -> str:
    stripped = (sql or "").strip().rstrip(";").strip()
    if "limit" in stripped.lower():
        return stripped
    return f"SELECT * FROM ({stripped}) LIMIT {int(max_rows)}"


def _validate_sql(sql: str) -> None:
    stripped = (sql or "").strip()
    if not _SQL_START_RE.match(stripped):
        raise ValueError("sql must start with SELECT or WITH")
    if ";" in stripped[:-1]:
        raise ValueError("sql must be a single statement (no internal semicolons)")
    if _SQL_DISALLOWED_RE.search(stripped):
        raise ValueError("sql contains disallowed statement/keyword")


@dataclass(frozen=True)
class SqlResult:
    payload: dict
    data_needs: list[dict]


class SqlRetrieverV2:
    def __init__(self, *, edgar_path: Path | None = None, real_estate_path: Path | None = None) -> None:
        self._paths = {
            "edgar": edgar_path or _resolve_dataset_path("edgar"),
            "real_estate": real_estate_path or _resolve_dataset_path("real_estate"),
        }

    def fetch(self, request: SqlRequest) -> SqlResult:
        params = request.params
        dataset = request.dataset
        if params.dialect != "sqlite":
            raise ValueError(f"unsupported sql dialect for v2 runtime: {params.dialect}")
        db_path = self._paths[dataset]
        if not db_path.exists():
            return SqlResult(
                payload={
                    "type": "sql",
                    "request_id": request.request_id,
                    "dataset": dataset,
                    "error": f"db not found: {db_path}",
                    "rows": [],
                    "columns": [],
                },
                data_needs=[
                    {
                        "name": f"sql:{dataset}",
                        "reason": f"database not found at {db_path}",
                        "criticality": "high",
                        "error_code": "missing_database",
                        "dataset": dataset,
                    }
                ],
            )

        _validate_sql(params.sql)
        sql = _wrap_limit(params.sql, params.max_rows)
        deadline = time.monotonic() + (params.timeout_ms / 1000.0)

        with _open_sqlite_ro(db_path) as conn:
            def progress_handler() -> int:
                return 1 if time.monotonic() > deadline else 0

            conn.set_progress_handler(progress_handler, 10_000)
            cur = conn.cursor()
            start_time = time.monotonic()
            cur.execute(sql)
            rows: list[dict[str, Any]] = []
            while True:
                batch = cur.fetchmany(500)
                if not batch:
                    break
                for row in batch:
                    rows.append(dict(row))
                    if len(rows) >= params.max_rows:
                        break
                if len(rows) >= params.max_rows:
                    break
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            columns = [desc[0] for desc in (cur.description or [])]

        payload = {
            "type": "sql",
            "request_id": request.request_id,
            "dataset": dataset,
            "columns": columns,
            "rows": rows,
            "meta": {
                "elapsed_ms": elapsed_ms,
                "max_rows": params.max_rows,
                "timeout_ms": params.timeout_ms,
                "concept_aliases": params.concept_aliases,
            },
        }
        if params.concept_aliases:
            payload["meta"]["concept_aliases"] = params.concept_aliases
        logger.info("sql_v2 dataset=%s rows=%s elapsed_ms=%s", dataset, len(rows), elapsed_ms)
        return SqlResult(payload=payload, data_needs=[])
