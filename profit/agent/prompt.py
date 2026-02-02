from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from profit.agent.types import Question, RetrievalPlan, RetrievedData


def build_messages(
    *,
    question: Question,
    plan: RetrievalPlan,
    data: RetrievedData,
    today: date | None = None,
    plan_opts: dict | None = None,
) -> list[dict]:
    plan_opts = plan_opts or {"max_points": 30, "decimals": 4}
    today = today or date.today()
    system = (
        "You are a data-bound assistant. "
        f"Today's date is {today.isoformat()}. "
        "Only answer using the provided data. "
        "If data is missing or outside the given window, say so."
    )

    plan_desc = {
        "source": plan.source,
        "instruments": plan.instruments,
        "regions": plan.regions,
        "filings": plan.filings,
        "window": {"start": plan.start.isoformat() if plan.start else None, "end": plan.end.isoformat() if plan.end else None},
        "notes": plan.notes,
    }
    # surface unresolved hints if retrievers reported them
    unresolved = []
    if isinstance(data.payload, dict):
        for key in ("unresolved", "unresolved_regions", "unresolved_filings"):
            vals = data.payload.get(key)
            if vals:
                unresolved.extend(vals if isinstance(vals, (list, tuple)) else [vals])

    data_block = _format_data_block(data, unresolved, plan_opts)

    user_prompt = (
        "User question:\n"
        f"{question.text}\n\n"
        "Retrieval plan:\n"
        f"{json.dumps(plan_desc)}\n\n"
        "Data:\n"
        f"{data_block}\n\n"
        "Instructions:\n"
        "- Use only the data provided; do not invent values.\n"
        "- State the date range and provider in your answer.\n"
        "- If unresolved inputs are listed, acknowledge they were not found.\n"
        "- Keep it concise (<=120 words)."
    )

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user_prompt},
    ]


def _format_data_block(data: RetrievedData, unresolved: list[str], plan_opts: dict) -> str:
    header = f"Retrieved data from {data.source} | window={data.start}..{data.end}"
    if data.payload is None:
        return header + "\n(no payload)"
    if data.source == "prices" and isinstance(data.payload, dict):
        return header + "\n" + _format_prices(data.payload, unresolved, plan_opts)
    if data.source == "redfin" and isinstance(data.payload, dict):
        return header + "\n" + _format_redfin(data.payload, unresolved)
    if data.source == "edgar" and isinstance(data.payload, dict):
        return header + "\n" + _format_edgar(data.payload, unresolved)
    return header + "\n" + _serialize_payload(data.payload)


def _serialize_payload(payload: Any, max_chars: int = 4000) -> str:
    try:
        text = json.dumps(payload, default=str)
    except TypeError:
        text = str(payload)
    if len(text) > max_chars:
        text = text[: max_chars - 3] + "..."
    return text


def _format_prices(payload: dict, unresolved: list[str], plan_opts: dict, max_points: int | None = None) -> str:
    max_points = max_points or plan_opts.get("max_points", 30)
    lines = []
    lines.append(f"provider={payload.get('provider')} unresolved={unresolved}")
    for inst in payload.get("instruments", []) or []:
        iid = inst.get("instrument_id")
        lines.append(f"[{iid}] providers_used={','.join(inst.get('providers_used', []) or [])}")
        fields = inst.get("fields", {}) or {}
        for field, series in fields.items():
            if not series:
                continue
            trimmed = series[-max_points:]
            compact = _compact_series(trimmed, decimals=plan_opts.get("decimals", 4))
            lines.append(f"  {field}: {compact}")
    return "\n".join(lines)


def _format_redfin(payload: dict, unresolved: list[str], max_rows: int = 20) -> str:
    lines = []
    lines.append(f"provider={payload.get('provider')} unresolved={unresolved}")
    for region in payload.get("regions", []) or []:
        rid = region.get("region_id")
        lines.append(f"[{rid} {region.get('name')}]")
        metrics = region.get("metrics", []) or []
        for row in metrics[:max_rows]:
            lines.append(
                f"  {row.get('period_start_date')} {row.get('period_granularity')}: "
                f"median_sale_price={row.get('median_sale_price')} inventory={row.get('inventory')}"
            )
    return "\n".join(lines)


def _format_edgar(payload: dict, unresolved: list[str], max_chunks: int = 10, max_facts: int = 10) -> str:
    lines = []
    lines.append(f"provider={payload.get('provider')} unresolved={unresolved}")
    facts = payload.get("facts") or []
    for entry in facts[:max_facts]:
        lines.append(f"[cik={entry.get('cik')} name={entry.get('name')}] facts={len(entry.get('facts', []))}")
        for fact in entry.get("facts", [])[:3]:
            lines.append(
                f"  {fact.get('report_id')} {fact.get('report_key')} {fact.get('period_end')}: "
                f"{fact.get('value')} {fact.get('units')}"
            )
    chunks = payload.get("chunks") or []
    for chunk in chunks[:max_chunks]:
        lines.append(f"chunk {chunk.get('file')} score={chunk.get('score',0)} text={chunk.get('text','')[:120]}")
    return "\n".join(lines)


def _compact_series(series: list[dict], max_groups: int = 10, decimals: int = 4) -> str:
    """
    Compact date=value pairs by grouping consecutive days with the same value.
    """
    if not series:
        return ""
    groups: list[tuple[str, str, float]] = []
    cur_start = cur_end = series[0]["ts"]
    cur_val = series[0]["value"]
    for row in series[1:]:
        ts = row["ts"]
        val = row["value"]
        prev_dt = _iso_to_date(cur_end)
        this_dt = _iso_to_date(ts)
        consecutive = (this_dt - prev_dt).days == 1 and val == cur_val
        if consecutive:
            cur_end = ts
            continue
        groups.append((cur_start, cur_end, cur_val))
        cur_start = cur_end = ts
        cur_val = val
    groups.append((cur_start, cur_end, cur_val))
    # limit groups
    groups = groups[-max_groups:]
    parts = []
    for start, end, val in groups:
        if start == end:
            parts.append(f"{start}={_fmt_num(val, decimals)}")
        else:
            parts.append(f"{start}..{end}={_fmt_num(val, decimals)}")
    return "; ".join(parts)


def _iso_to_date(iso: str):
    return date.fromisoformat(iso)


def _fmt_num(val: float, decimals: int = 4) -> str:
    try:
        return f"{float(val):.{decimals}f}"
    except Exception:
        return str(val)
