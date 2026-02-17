from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from agentapi.plan import Run
from agentapi.plan import Plan
from agentapi.runners import TransformRunner
from data_sources.edgar.edgar_data_store import EdgarDataStore

logger = logging.getLogger(__name__)

STAGE_DB_QUERY = "financial_adviser.db_query"


@dataclass(frozen=True)
class QueryRequest:
    key: str
    purpose: str
    sql: str


def _unique_key(desired: str, used: set[str]) -> str:
    base = (desired or "").strip() or "q"
    key = base
    i = 2
    while key in used:
        key = f"{base}_{i}"
        i += 1
    used.add(key)
    return key


def _coerce_query_requests(pending: object) -> list[QueryRequest]:
    """
    Read pending queries from user_context with backward compatibility.

    Supported:
      - ["SELECT ...", ...] (legacy)
      - [{"key": "...", "purpose": "...", "sql": "SELECT ..."}, ...]
    """

    if not isinstance(pending, list):
        return []

    used: set[str] = set()
    out: list[QueryRequest] = []
    for idx, item in enumerate(pending, start=1):
        if isinstance(item, str) and item.strip():
            out.append(QueryRequest(key=_unique_key(f"q{idx}", used), purpose="(unspecified)", sql=item.strip()))
            continue
        if isinstance(item, dict):
            sql = item.get("sql") or item.get("query")
            if not isinstance(sql, str) or not sql.strip():
                continue
            key = item.get("key")
            purpose = item.get("purpose")
            if not isinstance(key, str) or not key.strip():
                key = f"q{idx}"
            if not isinstance(purpose, str) or not purpose.strip():
                purpose = "(unspecified)"
            out.append(QueryRequest(key=_unique_key(key, used), purpose=purpose.strip(), sql=sql.strip()))
            continue
    return out


@dataclass(frozen=True)
class SqlResult:
    key: str
    purpose: str
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    error: str | None = None
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "purpose": self.purpose,
            "sql": self.sql,
            "columns": list(self.columns),
            "rows": [list(r) for r in self.rows],
            "error": self.error,
            "truncated": self.truncated,
        }


def _is_readonly_select(sql: str) -> bool:
    """
    Extremely defensive check that we only execute read-only SQL.

    We intentionally keep parsing simple: reject anything that looks like it
    might mutate the database or execute multiple statements.
    """

    s = (sql or "").strip()
    if not s:
        return False
    # Allow a single trailing semicolon (common when copy/pasting SQL), but
    # reject any other semicolons to prevent multi-statement execution.
    if ";" in s:
        stripped = s.rstrip()
        if stripped.endswith(";"):
            inner = stripped[:-1]
            if ";" in inner:
                return False
            s = inner.strip()
            s_low = s.lower().lstrip()
        else:
            return False
    s_low = s.lower().lstrip()
    if not (s_low.startswith("select") or s_low.startswith("with")):
        return False

    banned = (
        "insert ",
        "update ",
        "delete ",
        "drop ",
        "alter ",
        "create ",
        "pragma ",
        "attach ",
        "detach ",
        "vacuum",
        "reindex",
    )
    return not any(token in s_low for token in banned)


def _fetch_rows(conn, *, req: QueryRequest, limit: int) -> SqlResult:
    try:
        cur = conn.execute(req.sql)
        col_names = [d[0] for d in (cur.description or [])]
        raw_rows = cur.fetchmany(limit + 1)
        truncated = len(raw_rows) > limit
        raw_rows = raw_rows[:limit]
        rows = [list(r) for r in raw_rows]
        return SqlResult(key=req.key, purpose=req.purpose, sql=req.sql, columns=col_names, rows=rows, truncated=truncated)
    except Exception as exc:  # noqa: BLE001
        return SqlResult(key=req.key, purpose=req.purpose, sql=req.sql, columns=[], rows=[], error=str(exc), truncated=False)


class DbQueryStage(TransformRunner):
    """
    Executes SQL queries against our EDGAR SQLite database and stores results.

    This stage reads:
      user_context["financial_adviser"]["pending_queries"] : list[dict] (or legacy list[str])

    And appends:
      user_context["financial_adviser"]["db_results"] : list[dict]

    Then it returns control back to the LLM decision stage so the agent can
    request follow-up queries or finalize an answer.
    """

    name = STAGE_DB_QUERY

    def __init__(
        self,
        *,
        edgar_store: EdgarDataStore,
        next_stage_name: str,
        final_stage_name: str,
        max_round_trips: int = 6,
        max_rows_per_query: int = 50,
    ) -> None:
        self._edgar_store = edgar_store
        self._next_stage_name = next_stage_name
        self._final_stage_name = final_stage_name
        self._max_round_trips = max_round_trips
        self._max_rows_per_query = max_rows_per_query

    def run(self, *, previous_history_entries: list[Any], user_context: dict[str, Any]) -> Plan:
        user_context.setdefault("financial_adviser", {})
        fa = user_context["financial_adviser"]
        if not isinstance(fa, dict):
            raise ValueError("user_context.financial_adviser must be a dict")

        # Hard-stop runaway loops. The LLM stage increments `round` each time it
        # requests a query batch.
        round_num = int(fa.get("round", 0) or 0)
        if round_num >= self._max_round_trips:
            fa["answer"] = "I couldn't complete this within the query limit. Try rephrasing the question or narrowing the scope."
            fa["status"] = "completed"
            return Run(stage_name=self._final_stage_name)

        pending = fa.get("pending_queries")
        queries = _coerce_query_requests(pending)
        fa["pending_queries"] = []

        results_list = fa.get("db_results")
        if not isinstance(results_list, list):
            results_list = []
            fa["db_results"] = results_list

        if not queries:
            results_list.append(
                SqlResult(
                    key="no_pending_queries",
                    purpose="(internal)",
                    sql="",
                    columns=[],
                    rows=[],
                    error="no pending queries",
                    truncated=False,
                ).to_dict()
            )
            return Run(stage_name=self._next_stage_name)

        conn = self._edgar_store.connection
        for req in queries:
            if not _is_readonly_select(req.sql):
                results_list.append(
                    SqlResult(
                        key=req.key,
                        purpose=req.purpose,
                        sql=req.sql,
                        columns=[],
                        rows=[],
                        error="rejected: only single-statement SELECT/CTE queries are allowed",
                        truncated=False,
                    ).to_dict()
                )
                continue

            logger.info("Executing EDGAR SQL query key=%s len=%d", req.key, len(req.sql))
            res = _fetch_rows(conn, req=req, limit=self._max_rows_per_query)
            results_list.append(res.to_dict())

        return Run(stage_name=self._next_stage_name)
