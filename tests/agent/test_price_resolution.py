from datetime import date, datetime, timezone

from profit.agent import retrievers
from profit.agent.types import RetrievalPlan
from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.store import CatalogStore
from profit.sources.yfinance import PROVIDER
from profit.sources.yfinance_ingest import STEP_US, GRID_ORIGIN_US, WINDOW_POINTS


def _seed_catalog(db_path):
    catalog = CatalogStore(db_path)
    catalog.upsert_instruments(
        [
            # instrument_id matches columnar store series
            catalog._row_to_record(
                catalog.conn.execute(
                    "SELECT ? as instrument_id, 'equity' as instrument_type, ? as provider, ? as provider_code, NULL as mic, 'USD' as currency, NULL as active_from, NULL as active_to, '{}' as attrs",
                    ("XNAS|AAPL", PROVIDER, "AAPL"),
                ).fetchone()
            )
        ]
    )
    catalog.upsert_provider_mapping(instrument_id="XNAS|AAPL", provider=PROVIDER, provider_code="AAPL")
    return catalog


def _seed_series(db_path):
    store = ColumnarSqliteStore(db_path)
    sid = store.create_series(
        instrument_id="XNAS|AAPL",
        field="close",
        provider_id=PROVIDER,
        step_us=STEP_US,
        grid_origin_ts_us=GRID_ORIGIN_US,
        window_points=WINDOW_POINTS,
        compression="zlib",
        offsets_enabled=False,
        checksum_enabled=True,
    )
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    store.write(sid, [(ts, 100.0)])
    store.flush()
    return store


def test_price_retriever_resolves_ticker_via_catalog(tmp_path):
    catalog_db = tmp_path / "profit.sqlite"
    col_db = tmp_path / "col.sqlite3"
    _seed_catalog(catalog_db)
    store = _seed_series(col_db)

    plan = RetrievalPlan(source="prices", instruments=("AAPL",), start=date(2024, 1, 1), end=date(2024, 1, 2))
    result = retrievers.fetch(plan, columnar_store=store, catalog_db_path=catalog_db)
    payload = result.payload

    instrument = payload["instruments"][0]
    assert instrument["instrument_id"] == "XNAS|AAPL"
    assert instrument["fields"]["close"][0]["value"] == 100.0
    assert instrument["fields"]["close"][0]["provider"] == "yfinance"
    assert payload["unresolved"] == []
    assert payload["provider"] == "multi"
