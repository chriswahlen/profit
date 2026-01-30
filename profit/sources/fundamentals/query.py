from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Mapping, Any

from profit.cache import SqliteStore


def _to_utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def read_asof(
    store: SqliteStore,
    *,
    instrument_id: str,
    asof: datetime,
    tags: Iterable[str] | None = None,
    statement: str | None = None,
    period_start_from: datetime | None = None,
    period_end_from: datetime | None = None,
    period_end_to: datetime | None = None,
    totals_only: bool = False,
    limit: int | None = None,
    decode_text: bool = True,
) -> list[Mapping[str, Any]]:
    """
    Point-in-time read of fundamentals facts.

    For each fact identity key, returns the row with the latest known_at <= asof.
    """
    params: list[Any] = [_to_utc_iso(asof), instrument_id]
    where_clauses = [
        "known_at <= ?",
        "instrument_id = ?",
    ]
    if totals_only:
        where_clauses.append("(dims_hash IS NULL OR dims_hash = '')")
    if tags:
        tags_list = list(tags)
        placeholders = ",".join(["?"] * len(tags_list))
        where_clauses.append(f"tag_qname IN ({placeholders})")
        params.extend(tags_list)
    if statement:
        where_clauses.append("statement = ?")
        params.append(statement)
    if period_start_from:
        where_clauses.append("period_start >= ?")
        params.append(_to_utc_iso(period_start_from))
    if period_end_from:
        where_clauses.append("period_end >= ?")
        params.append(_to_utc_iso(period_end_from))
    if period_end_to:
        where_clauses.append("period_end <= ?")
        params.append(_to_utc_iso(period_end_to))

    where_sql = " AND ".join(where_clauses)
    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""

    sql = f"""
    WITH candidates AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY instrument_id, tag_qname, period_start, period_end, unit, dims_hash, value_kind
                ORDER BY known_at DESC
            ) AS rn
        FROM "fundamentals_fact:sec:v1"
        WHERE {where_sql}
    )
    SELECT * FROM candidates WHERE rn = 1{limit_sql};
    """

    rows = store.query(sql, params=params, as_dataframe=False)
    if decode_text:
        for row in rows:
            if row.get("value_kind") == "text":
                row["value_text"] = None
                gz = row.get("value_text_gz")
                if gz:
                    try:
                        import gzip
                    except ModuleNotFoundError:
                        continue
                    try:
                        row["value_text"] = gzip.decompress(gz).decode("utf-8")
                    except Exception:
                        row["value_text"] = None
    return rows
