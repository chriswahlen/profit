from datetime import date, datetime, timezone

from profit.agent import retrievers
from profit.agent.types import RetrievalPlan
from profit.cache.columnar_store import ColumnarSqliteStore
from profit.sources.yfinance import PROVIDER as YF_PROVIDER


def test_price_retriever_prefers_higher_priority_provider(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # lower priority provider stooq
    sid_s = store.create_series(
        instrument_id="XNAS|AAPL",
        field="close",
        provider_id="stooq",
        step_us=86_400_000_000,
        grid_origin_ts_us=int(datetime(1900, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000),
        window_points=1095,
        compression="zlib",
        offsets_enabled=False,
        checksum_enabled=True,
    )
    store.write(sid_s, [(ts, 90.0)])

    # higher priority provider yfinance
    sid_y = store.create_series(
        instrument_id="XNAS|AAPL",
        field="close",
        provider_id=YF_PROVIDER,
        step_us=86_400_000_000,
        grid_origin_ts_us=int(datetime(1900, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000),
        window_points=1095,
        compression="zlib",
        offsets_enabled=False,
        checksum_enabled=True,
    )
    store.write(sid_y, [(ts, 100.0)])
    store.flush()

    plan = RetrievalPlan(source="prices", instruments=("XNAS|AAPL",), start=date(2024, 1, 1), end=date(2024, 1, 1))
    result = retrievers.fetch(plan, columnar_store=store)
    payload = result.payload
    close = payload["instruments"][0]["fields"]["close"][0]
    assert close["value"] == 100.0
    assert close["provider"] == YF_PROVIDER
