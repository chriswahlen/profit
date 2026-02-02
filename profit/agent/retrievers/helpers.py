from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_date_bound(value: str | None) -> datetime | None:
    if value is None or value == "null":
        return None
    parsed = datetime.fromisoformat(value)
    return _ensure_utc(parsed)


def normalize_window(start: datetime | None, end: datetime | None, *, default_span: timedelta) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    else:
        end = _ensure_utc(end)
    if start is None:
        start = end - default_span
    else:
        start = _ensure_utc(start)
    if start > end:
        start, end = end - default_span, end
    return start, end


AGG_PATTERN = re.compile(r"(?:(\d+)d|weekly|monthly)_(avg|median|max|min)")


def compute_aggregations(
    points: Sequence[tuple[datetime, float]],
    *,
    aggregations: Iterable[str],
    window_end: datetime,
) -> dict[str, float | None]:
    results: dict[str, float | None] = {}
    sorted_points = sorted(points, key=lambda p: p[0])
    for agg in aggregations:
        match = AGG_PATTERN.fullmatch(agg)
        if not match:
            results[agg] = None
            continue
        window_token, op = match.group(1), match.group(2)
        if window_token is not None:
            window_days = int(window_token)
        elif agg.startswith("weekly_"):
            window_days = 7
        elif agg.startswith("monthly_"):
            window_days = 30
        else:
            window_days = 30
        window_delta = timedelta(days=window_days)
        window_start = window_end - window_delta + timedelta(seconds=1)
        window_values = [
            value
            for ts, value in sorted_points
            if window_start <= ts <= window_end
        ]
        if not window_values:
            results[agg] = None
            continue

        if op == "avg":
            results[agg] = sum(window_values) / len(window_values)
        elif op == "median":
            values = sorted(window_values)
            mid = len(values) // 2
            if len(values) % 2 == 1:
                results[agg] = values[mid]
            else:
                results[agg] = (values[mid - 1] + values[mid]) / 2
        elif op == "max":
            results[agg] = max(window_values)
        elif op == "min":
            results[agg] = min(window_values)
        else:
            results[agg] = None
    return results
