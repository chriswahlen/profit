from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from profit.sources.redfin_dump_ingest import (
    RedfinIngestConfig,
    ingest_redfin_rows,
    record_ingestion_run,
    rows_from_dump,
)
from profit.stores.redfin_store import RedfinStore


def test_rows_from_dump_normalizes_headers(tmp_path: Path) -> None:
    path = tmp_path / "sample.tsv"
    path.write_text(
        "RegionID\tRegionName\tPeriod_Begin\n"
        "zip|10001\tNew York Zip\t2025-01-06\n",
        encoding="utf-8",
    )

    rows = list(rows_from_dump(path, delimiter="\t", limit=None))
    assert rows == [
        {
            "regionid": "zip|10001",
            "regionname": "New York Zip",
            "period_begin": "2025-01-06",
        }
    ]


def test_ingest_redfin_rows_populates_tables(tmp_path: Path) -> None:
    store = RedfinStore(tmp_path / "redfin.sqlite")
    rows = [
        {
            "REGIONID": "zip|10001",
            "REGIONNAME": "New York Zip",
            "REGIONTYPE": "Zip Code",
            "PERIOD_BEGIN": "2025-01-06",
            "MEDIAN_SALE_PRICE": "500000",
            "MEDIAN_LIST_PRICE": "520000",
            "HOMES_SOLD": "15",
            "NEW_LISTINGS": "20",
            "INVENTORY": "80",
            "MEDIAN_DOM": "25",
            "AVG_SALE_TO_LIST": "0.98",
            "PRICE_DROPS": "2",
            "PENDING_SALES": "12",
            "MONTHS_OF_SUPPLY": "3.5",
            "MEDIAN_PPSF": "210",
            "STATE": "NY",
        }
    ]
    config = RedfinIngestConfig(period_granularity="week", country_iso2="US", default_data_revision=5)
    start = datetime(2025, 1, 6, tzinfo=timezone.utc)
    stats = ingest_redfin_rows(conn=store.conn, rows=rows, config=config, run_started_at=start)

    assert stats.row_count == 1
    assert stats.regions == 1
    assert stats.max_data_revision == 5

    cursor = store.conn.cursor()
    assert cursor.execute("SELECT region_id FROM regions").fetchone()[0] == "zip|10001"
    provider_row = cursor.execute(
        "SELECT provider, data_revision FROM region_provider_map WHERE provider_region_id='zip|10001'"
    ).fetchone()
    assert provider_row == ("redfin", 5)

    metadata = json.loads(cursor.execute("SELECT metadata FROM regions").fetchone()[0])
    assert metadata["state"] == "NY"

    metrics = cursor.execute("SELECT median_sale_price, sale_to_list_ratio, price_drops_pct FROM market_metrics").fetchone()
    assert metrics == (500000.0, 0.98, 2.0)

    record_ingestion_run(
        conn=store.conn,
        run_id="test",
        provider="redfin",
        started_at=start,
        finished_at=start,
        status="success",
        source_url=None,
        row_count=1,
        data_revision=5,
    )
    assert cursor.execute("SELECT COUNT(*) FROM ingestion_runs").fetchone()[0] == 1
