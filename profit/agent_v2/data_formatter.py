from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


MAX_ROWS = 200


def format_data_block(payloads: list[Mapping[str, Any]]) -> str:
    """
    Convert retrieved payloads into a compact, LLM-friendly DATA block.
    """
    if not payloads:
        return "DATA\n(no data)"
    chunks: list[str] = []
    for payload in payloads:
        typ = str(payload.get("type") or "unknown")
        if typ == "market":
            chunks.append(_format_market(payload))
        elif typ == "sql":
            chunks.append(_format_sql(payload))
        else:
            chunks.append(_format_generic(payload))
    return "DATA\n" + "\n\n".join(chunks)


def _format_market(payload: Mapping[str, Any]) -> str:
    data = payload.get("data") or []
    if not data:
        return "market: (no data)"
    lines: list[str] = ["market:"]
    for series in data:
        instrument = series.get("instrument", "?")
        field = series.get("field", "?")
        points = series.get("points") or []
        lines.append(f"- {instrument} {field}: {len(points)} points")
        for point in points[: min(MAX_ROWS, 10)]:
            ts = point.get("timestamp")
            value = point.get("value")
            if ts is None or value is None:
                continue
            lines.append(f"  {str(ts)[:10]} {value}")
    return "\n".join(lines)


def _format_sql(payload: Mapping[str, Any]) -> str:
    dataset = payload.get("dataset") or "unknown"
    columns = payload.get("columns") or []
    rows = payload.get("rows") or []
    lines: list[str] = [f"sql[{dataset}]: rows={len(rows)} cols={len(columns)}"]
    if columns:
        lines.append("columns: " + ", ".join(map(str, columns)))
    for row in rows[: min(MAX_ROWS, 20)]:
        if isinstance(row, Mapping):
            compact = {k: row.get(k) for k in list(columns)[:12]} if columns else dict(row)
        else:
            compact = row
        lines.append(json.dumps(compact, ensure_ascii=False, separators=(",", ":"), default=str))
    if len(rows) > 20:
        lines.append(f"... (+{len(rows) - 20} more rows)")
    return "\n".join(lines)


def _format_generic(payload: Mapping[str, Any]) -> str:
    try:
        compact = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        compact = "{}"
    if len(compact) > 4000:
        compact = compact[:2000] + "..." + compact[-500:]
    return f"generic: {compact}"

