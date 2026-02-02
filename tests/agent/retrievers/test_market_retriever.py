from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from profit.agent.retrievers.market import MarketRetriever
from profit.cache.columnar_store import ColumnarSqliteStore


def _make_points(start: datetime, count: int) -> list[tuple[datetime, float]]:
    return [(start + timedelta(days=i), float(100 + i * 5)) for i in range(count)]


def test_returns_data_and_aggregations(tmp_path: Path) -> None:
    store = ColumnarSqliteStore(tmp_path / "col.sqlite")
    origin = datetime(2025, 1, 1, tzinfo=timezone.utc)
    series_id = store.create_series(
        instrument_id="XNAS|DAT",
        field="close",
        step_us=86400 * 1_000_000,
        grid_origin_ts_us=int(origin.timestamp() * 1_000_000),
        window_points=32,
    )
    store.write(series_id, _make_points(origin, 3))
    store.flush()

    retriever = MarketRetriever(store=store)
    request = {
        "instruments": ["XNAS|DAT"],
        "fields": ["close"],
        "start": "2025-01-01",
        "end": "2025-01-03",
        "aggregation": ["7d_avg"],
    }
    result = retriever.fetch(request)
    assert len(result.payload["data"]) == 1
    avg_value = result.payload["data"][0]["aggregations"]["7d_avg"]
    assert avg_value and avg_value > 100


def test_reports_missing_data(tmp_path: Path) -> None:
    store = ColumnarSqliteStore(tmp_path / "col.sqlite")
    retriever = MarketRetriever(store=store)
    request = {
        "instruments": ["XNAS|MISSING"],
        "fields": ["close"],
        "start": "2025-01-01",
        "end": "2025-01-03",
        "aggregation": ["7d_avg"],
    }
    result = retriever.fetch(request)
    assert result.data_needs
    assert "MISSING" in result.data_needs[0]["name"]


def test_combines_multiple_series_for_same_instrument(tmp_path: Path) -> None:
    store = ColumnarSqliteStore(tmp_path / "col.sqlite")
    origin = datetime(2025, 1, 1, tzinfo=timezone.utc)
    series_a = store.create_series(
        instrument_id="XNAS|COMBO",
        field="close",
        step_us=86400 * 1_000_000,
        grid_origin_ts_us=int(origin.timestamp() * 1_000_000),
        window_points=32,
    )
    series_b = store.create_series(
        instrument_id="XNAS|COMBO",
        field="close",
        step_us=86400 * 1_000_000,
        grid_origin_ts_us=int(origin.timestamp() * 1_000_000),
        window_points=32,
        provider_id="alternate",
    )
    store.write(series_a, _make_points(origin, 2))
    store.write(series_b, _make_points(origin + timedelta(days=2), 2))
    store.flush()

    retriever = MarketRetriever(store=store)
    request = {
        "instruments": ["XNAS|COMBO"],
        "fields": ["close"],
        "start": "2025-01-01",
        "end": "2025-01-04",
        "aggregation": ["7d_avg"],
    }
    result = retriever.fetch(request)
    assert len(result.payload["data"]) == 1
    points = result.payload["data"][0]["points"]
    assert len(points) == 4
