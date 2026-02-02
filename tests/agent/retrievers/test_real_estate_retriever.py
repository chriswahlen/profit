from __future__ import annotations

from datetime import datetime
from pathlib import Path

from profit.agent.retrievers.real_estate import RealEstateRetriever
from profit.stores.redfin_store import RedfinStore


def _insert_region(store: RedfinStore, region_id: str) -> None:
    store.conn.execute(
        """
        INSERT INTO regions (region_id, region_type, name, canonical_code, country_iso2, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (region_id, "metro", region_id, region_id.upper(), "US", "2025-01-01T00:00:00Z"),
    )


def _insert_metric(store: RedfinStore, region_id: str, date_str: str) -> None:
    store.conn.execute(
        """
        INSERT INTO market_metrics (
            region_id, period_start_date, period_granularity, data_revision, source_provider,
            median_sale_price, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (region_id, date_str, "week", 1, "redfin", 500000, "2025-01-01T00:00:00Z"),
    )


def test_returns_metrics_and_aggregations(tmp_path: Path) -> None:
    db_path = tmp_path / "redfin.sqlite"
    store = RedfinStore(db_path, readonly=False)
    store.conn.execute(
        """
        INSERT INTO regions (region_id, region_type, name, canonical_code, country_iso2, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("metro|us|live", "metro", "Live Metro", "LIVE", "US", "2025-01-01T00:00:00Z"),
    )
    store.conn.execute(
        """
        INSERT INTO market_metrics (
            region_id, period_start_date, period_granularity, data_revision, source_provider,
            median_sale_price, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("metro|us|live", "2025-01-01", "week", 1, "redfin", 550000, "2025-01-01T00:00:00Z"),
    )
    store.conn.commit()

    retriever = RealEstateRetriever(store=store)
    request = {
        "regions": ["metro|us|live"],
        "start": "2025-01-01",
        "end": "2025-01-07",
        "aggregation": ["weekly_avg"],
    }
    result = retriever.fetch(request)
    assert result.payload["data"]
    assert result.payload["data"][0]["aggregations"]["weekly_avg"] == 550000


def test_reports_missing_region(tmp_path: Path) -> None:
    store = RedfinStore(tmp_path / "redfin.sqlite", readonly=False)
    retriever = RealEstateRetriever(store=store)
    request = {
        "regions": ["metro|us|missing"],
        "start": "2025-01-01",
        "end": "2025-01-07",
        "aggregation": ["weekly_avg"],
    }
    result = retriever.fetch(request)
    assert result.data_needs
    assert "missing" in result.data_needs[0]["name"]
    assert result.data_needs[0]["error_code"] == "region_no_data"


def test_returns_multiple_regions_and_reports_missing(tmp_path: Path) -> None:
    store = RedfinStore(tmp_path / "redfin.sqlite", readonly=False)
    _insert_region(store, "metro|us|live")
    _insert_region(store, "metro|us|east")
    _insert_metric(store, "metro|us|live", "2025-01-02")
    _insert_metric(store, "metro|us|east", "2025-01-03")
    _insert_metric(store, "metro|us|east", "2025-01-10")
    store.conn.commit()

    retriever = RealEstateRetriever(store=store)
    request = {
        "regions": ["metro|us|live", "metro|us|east", "metro|us|ghost"],
        "start": "2025-01-01",
        "end": "2025-01-08",
        "aggregation": ["weekly_avg"],
    }
    result = retriever.fetch(request)
    assert len(result.payload["data"]) == 2
    assert {entry["region"] for entry in result.payload["data"]} == {
        "metro|us|live",
        "metro|us|east",
    }
    assert any("ghost" in need["name"] for need in result.data_needs)
