from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from profit.agent.retrievers.real_estate import RealEstateRetriever
from profit.stores.redfin_store import RedfinStore


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
