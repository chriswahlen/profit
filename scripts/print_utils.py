from __future__ import annotations

from datetime import datetime
from typing import Iterable

from profit.cache import ColumnarSqliteStore


def print_points(
    store: ColumnarSqliteStore,
    dataset: str,
    instrument_id: str,
    fields: Iterable[str],
    start: datetime,
    end: datetime,
    *,
    step_us: int,
    provider_id: str,
) -> None:
    for field in fields:
        series_id = store.get_series_id(
            instrument_id=instrument_id,
            field=field,
            step_us=step_us,
            provider_id=provider_id,
        )
        if series_id is None:
            print(f"No series for {field} (dataset={dataset})")
            continue

        points = store.read_points(
            series_id,
            start=start,
            end=end,
            include_sentinel=False,
        )
        if not points:
            print(f"No points for {field} in requested window.")
            continue

        print(f"Stored {len(points)} points for {field}:")
        for ts, value in points:
            print(f"  {ts.date().isoformat()} {value}")
