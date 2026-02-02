from datetime import datetime
from pathlib import Path

import pytest

from profit.stores.redfin_store import RedfinStore


def _insert_region(store: RedfinStore, region_id: str) -> None:
    store.conn.execute(
        """
        INSERT INTO regions (
            region_id, region_type, name, canonical_code, country_iso2, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (region_id, "metro", region_id, region_id.upper(), "US", "2025-01-01T00:00:00Z"),
    )


def _insert_metric(store: RedfinStore, region_id: str, date_str: str) -> None:
    store.conn.execute(
        """
        INSERT INTO market_metrics (
            region_id, period_start_date, period_granularity, data_revision,
            source_provider, median_sale_price, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (region_id, date_str, "week", 1, "redfin", 550000, "2025-01-01T00:00:00Z"),
    )


def test_fetch_market_metrics_returns_rows(tmp_path: Path) -> None:
    store = RedfinStore(tmp_path / "redfin.sqlite", readonly=False)
    _insert_region(store, "metro|us|live")
    _insert_metric(store, "metro|us|live", "2025-01-01")
    store.conn.commit()

    rows = store.fetch_market_metrics(
        ["metro|us|live"],
        start_date="2025-01-01",
        end_date="2025-01-07",
    )

    assert len(rows) == 1
    assert rows[0]["region_id"] == "metro|us|live"
    assert rows[0]["median_sale_price"] == 550000


def test_fetch_market_metrics_empty_when_no_regions(tmp_path: Path) -> None:
    store = RedfinStore(tmp_path / "redfin.sqlite", readonly=False)
    assert store.fetch_market_metrics([], start_date="2025-01-01", end_date="2025-01-07") == []


def test_fetch_market_metrics_invalid_window_raises(tmp_path: Path) -> None:
    store = RedfinStore(tmp_path / "redfin.sqlite", readonly=False)
    with pytest.raises(ValueError):
        store.fetch_market_metrics(
            ["metro|us|live"],
            start_date="2025-01-07",
            end_date="2025-01-01",
        )


def test_fetch_market_metrics_multiple_regions_and_boundaries(tmp_path: Path) -> None:
    store = RedfinStore(tmp_path / "redfin.sqlite", readonly=False)
    _insert_region(store, "metro|us|live")
    _insert_region(store, "metro|us|east")
    _insert_region(store, "metro|us|ghost")
    _insert_metric(store, "metro|us|live", "2025-01-02")
    _insert_metric(store, "metro|us|east", "2025-01-03")
    # Out-of-window row should be skipped.
    _insert_metric(store, "metro|us|east", "2025-01-10")
    store.conn.commit()

    rows = store.fetch_market_metrics(
        ["metro|us|live", "metro|us|east", "metro|us|ghost"],
        start_date="2025-01-01",
        end_date="2025-01-08",
    )

    assert {row["region_id"] for row in rows} == {"metro|us|live", "metro|us|east"}
    assert all(row["period_start_date"] <= "2025-01-08" for row in rows)
