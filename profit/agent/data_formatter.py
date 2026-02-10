from __future__ import annotations

import json
from typing import Any, Iterable


def _format_market_entry(entry: dict) -> str:
    instrument = entry.get("instrument", "")
    field = entry.get("field", "")
    header = f"{instrument} | {field}".strip(" |")
    lines = [header]
    points: Iterable[dict] = entry.get("points") or []
    for point in points:
        ts = point.get("timestamp", "")
        value = point.get("value", "")
        lines.append(f"{ts.split('T')[0]}: {float(value):.2f}")
    aggregations = entry.get("aggregations") or {}
    if aggregations:
        lines.append(f"aggregations: {json.dumps(aggregations, ensure_ascii=False, sort_keys=True)}")
    return "\n".join(lines)


def format_data_block(payload: dict[str, Any]) -> str:
    """Render structured data into a compact textual block for prompts."""
    kind = payload.get("type", "unknown")
    body_lines: list[str] = []

    if kind == "market":
        for entry in payload.get("data", []) or []:
            body_lines.append(_format_market_entry(entry))
    else:
        body_lines.append(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True))

    return "DATA\n" + "\n\n".join(body_lines)


__all__ = ["format_data_block"]
