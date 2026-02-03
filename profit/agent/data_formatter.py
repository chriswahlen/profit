from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime
from typing import Any

MAX_ROWS = 300
AGG_PATTERN = re.compile(r"(?:(\d+)d|weekly|monthly)_(avg|median|max|min)")


def format_data_block(payload: Mapping[str, Any]) -> str:
    if payload.get("type") == "market":
        body = _format_market_payload(payload)
    else:
        body = _format_generic_payload(payload)
    return "DATA\n" + body


def _format_market_payload(payload: Mapping[str, Any]) -> str:
    data = payload.get("data") or []
    if not data:
        return "(no market data)"

    instrument_fields: dict[str, list[str]] = defaultdict(list)
    instrument_points: dict[str, dict[str, list[tuple[str, float]]]] = defaultdict(lambda: defaultdict(list))
    requested_aggs = _collect_aggregations(data)

    for entry in data:
        instrument = entry.get("instrument")
        field = entry.get("field")
        if not instrument or not field:
            continue
        instrument_fields[instrument].append(field)
        for point in entry.get("points") or []:
            ts = point.get("timestamp")
            value = point.get("value")
            if not ts or value is None:
                continue
            instrument_points[instrument][field].append((ts, float(value)))

    agg_rows = _compute_aggregations(instrument_points, requested_aggs)
    lines: list[str] = []
    for instrument in sorted(instrument_fields):
        fields = _dedupe_preserve_order(instrument_fields[instrument])
        if not fields:
            continue
        header = f"{instrument}"
        if requested_aggs:
            header += " / " + ",".join(requested_aggs)
        header += " (" + ",".join(fields) + ")" if fields else ""
        header += ":"
        lines.append(header)

        if requested_aggs:
            instrument_aggs = agg_rows.get(instrument, {})
            for agg_name in requested_aggs:
                periods = instrument_aggs.get(agg_name) or {}
                for period in sorted(periods):
                    values = periods[period]
                    parts = [f"{field}={values.get(field, '')}" for field in fields]
                    lines.append(f"{period} {agg_name}: {','.join(parts)}")
            lines.append("")
            continue

        sorted_dates = sorted(
            {date for vals in instrument_points[instrument].values() for date in {ts[:10] for ts, _ in vals}}
        )
        remaining = len(sorted_dates)
        for date in sorted_dates[:MAX_ROWS]:
            values = [
                _find_value_on_date(instrument_points[instrument][field], date)
                for field in fields
            ]
            lines.append(f"{date}: {','.join(values)}")
        if remaining > MAX_ROWS:
            lines.append(f"... (+{remaining - MAX_ROWS} more dates)")
        lines.append("")
    return "\n".join(line for line in lines if line)


def _compute_aggregations(
    points: dict[str, dict[str, list[tuple[str, float]]]],
    aggs: list[str],
) -> dict[str, dict[str, dict[str, dict[str, str]]]]:
    result: dict[str, dict[str, dict[str, dict[str, str]]]] = defaultdict(lambda: defaultdict(dict))
    for instrument, field_map in points.items():
        for field, records in field_map.items():
            for agg_name in aggs:
                buckets = _bucket(records, agg_name)
                for period, values in buckets.items():
                    formatted = _apply_agg(values, agg_name)
                    result[instrument][agg_name].setdefault(period, {})[field] = formatted
    return result


def _bucket(records: list[tuple[str, float]], agg_name: str) -> dict[str, list[float]]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for ts, value in records:
        period = _period_label(ts, agg_name)
        buckets[period].append(value)
    return buckets


def _period_label(timestamp: str, agg_name: str) -> str:
    if agg_name.startswith("monthly_"):
        return timestamp[:7]
    if agg_name.startswith("weekly_"):
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return f"week-{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
    match = AGG_PATTERN.fullmatch(agg_name)
    if match and match.group(1):
        return timestamp[:10]
    return timestamp[:10]


def _apply_agg(values: list[float], agg_name: str) -> str:
    if not values:
        return ""
    op = agg_name.split("_", 1)[-1]
    if op == "avg":
        return f"{sum(values) / len(values):.2f}"
    if op == "median":
        sorted_vals = sorted(values)
        mid = len(sorted_vals) // 2
        if len(sorted_vals) % 2 == 1:
            return f"{sorted_vals[mid]:.2f}"
        return f"{(sorted_vals[mid - 1] + sorted_vals[mid]) / 2:.2f}"
    if op == "max":
        return f"{max(values):.2f}"
    if op == "min":
        return f"{min(values):.2f}"
    return ""


def _find_value_on_date(records: list[tuple[str, float]], date: str) -> str:
    for ts, value in records:
        if ts.startswith(date):
            return _format_value(value)
    return ""


def _collect_aggregations(data: list[dict]) -> list[str]:
    seen: list[str] = []
    for entry in data:
        for agg in entry.get("aggregations") or {}:
            if agg not in seen:
                seen.append(agg)
    return seen


def _format_generic_payload(payload: Mapping[str, Any]) -> str:
    try:
        compact = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
    except Exception:
        compact = "{}"
    return compact


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, int):
        return str(value)
    return str(value)
