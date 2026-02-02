from datetime import date, datetime, timezone

from profit.agent import retrievers
from profit.agent.types import RetrievalPlan
from profit.cache.columnar_store import ColumnarSqliteStore
from profit.sources.yfinance import FIELD_ORDER, PROVIDER
from profit.sources.yfinance_ingest import GRID_ORIGIN_US, STEP_US, WINDOW_POINTS


def _seed_store(tmp_path) -> ColumnarSqliteStore:
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    for field, values in {"close": [100.0, 101.0], "volume": [1_000_000.0, 1_200_000.0]}.items():
        sid = store.create_series(
            instrument_id="XNAS|AAPL",
            field=field,
            provider_id=PROVIDER,
            step_us=STEP_US,
            grid_origin_ts_us=GRID_ORIGIN_US,
            window_points=WINDOW_POINTS,
            compression="zlib",
            offsets_enabled=False,
            checksum_enabled=True,
        )
        store.write(sid, [(ts1, values[0]), (ts2, values[1])])
    store.flush()
    return store


def test_fetch_prices_reads_columnar_store(tmp_path):
    store = _seed_store(tmp_path)
    plan = RetrievalPlan(
        source="prices",
        instruments=("XNAS|AAPL",),
        start=date(2024, 1, 1),
        end=date(2024, 1, 2),
    )

    result = retrievers.fetch(plan, columnar_store=store)
    payload = result.payload

    assert payload["provider"] == "multi"
    assert payload["field_order"] == FIELD_ORDER
    assert payload["window"]["start"] == "2024-01-01"
    assert payload["window"]["end"] == "2024-01-02"

    instrument = payload["instruments"][0]
    fields = instrument["fields"]

    assert instrument["instrument_id"] == "XNAS|AAPL"
    assert instrument["missing_fields"]  # open/high/low/adj_close not seeded

    close_points = fields["close"]
    volume_points = fields["volume"]

    assert close_points == [
        {"ts": "2024-01-01", "value": 100.0, "provider": "yfinance"},
        {"ts": "2024-01-02", "value": 101.0, "provider": "yfinance"},
    ]
    assert volume_points[-1]["value"] == 1_200_000.0
