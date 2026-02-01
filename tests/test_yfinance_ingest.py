from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from profit.cache import FileCache
from profit.catalog.types import InstrumentRecord
from profit.config import ProfitConfig
from profit.sources.yfinance_ingest import fetch_and_store_yfinance
from profit.stores import StoreContainer


def _cfg(base, db_path):
    return ProfitConfig(
        data_root=base,
        cache_root=base,
        store_path=db_path,
        log_level="INFO",
        refresh_catalog=False,
    )


def _upsert_instrument(catalog, ticker: str, instrument_id: str, provider_code: str | None = None):
    rec = InstrumentRecord(
        instrument_id=instrument_id,
        instrument_type="equity",
        provider="yfinance",
        provider_code=provider_code or ticker,
        mic="XNYS",
        currency="USD",
        active_from=datetime(1900, 1, 1, tzinfo=timezone.utc),
        active_to=None,
        attrs={},
    )
    catalog.upsert_instruments([rec])


def test_fetch_and_store_writes_columnar(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    stores = StoreContainer.open(db_path)
    _upsert_instrument(stores.catalog, "AAPL", "EQ|AAPL")

    idx = pd.date_range("2024-01-01", periods=2, freq="D", tz=timezone.utc)
    df = pd.DataFrame(
        {
            "Open": [1.0, 2.0],
            "High": [1.5, 2.5],
            "Low": [0.5, 1.5],
            "Close": [1.2, 2.2],
            "Adj Close": [1.1, 2.1],
            "Volume": [100, 200],
        },
        index=idx,
    )

    def fake_download(tickers, start, end, interval):
        return df

    cfg = _cfg(tmp_path, db_path)
    cache = FileCache(base_dir=tmp_path)

    fetch_and_store_yfinance(
        instrument_ids=["EQ|AAPL"],
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 2, tzinfo=timezone.utc),
        cfg=cfg,
        stores=stores,
        cache=cache,
        ttl=timedelta(days=1),
        download_fn=fake_download,
    )

    sid = stores.columnar.get_series_id(
        instrument_id="EQ|AAPL", field="close", step_us=86_400_000_000
    )
    assert sid is not None
    points = stores.columnar.read_points(sid, start=idx[0], end=idx[-1], include_sentinel=False)
    assert len(points) == 2
    assert points[0][1] == 1.2
    stores.close()


def test_missing_instrument_raises(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    stores = StoreContainer.open(db_path)
    cfg = _cfg(tmp_path, db_path)
    cache = FileCache(base_dir=tmp_path)

    with pytest.raises(RuntimeError):
        fetch_and_store_yfinance(
            instrument_ids=["EQ|MSFT"],
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 2, tzinfo=timezone.utc),
            cfg=cfg,
            stores=stores,
            cache=cache,
            ttl=timedelta(days=1),
            download_fn=lambda *args, **kwargs: pd.DataFrame(),
        )
    stores.close()


def test_dry_run_skips_writes(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    stores = StoreContainer.open(db_path)
    _upsert_instrument(stores.catalog, "AAPL", "EQ|AAPL")

    idx = pd.date_range("2024-01-01", periods=1, freq="D", tz=timezone.utc)
    df = pd.DataFrame(
        {"Open": [1.0], "High": [1.5], "Low": [0.5], "Close": [1.2], "Adj Close": [1.1], "Volume": [100]},
        index=idx,
    )

    cfg = _cfg(tmp_path, db_path)
    cache = FileCache(base_dir=tmp_path)

    fetch_and_store_yfinance(
        instrument_ids=["EQ|AAPL"],
        start=idx[0],
        end=idx[0],
        cfg=cfg,
        stores=stores,
        cache=cache,
        ttl=timedelta(days=1),
        download_fn=lambda *args, **kwargs: df,
        dry_run=True,
    )

    sid = stores.columnar.get_series_id(
        instrument_id="EQ|AAPL", field="close", step_us=86_400_000_000
    )
    assert sid is None
    stores.close()


def test_lookup_with_prefixed_provider_code(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    stores = StoreContainer.open(db_path)
    cfg = _cfg(tmp_path, db_path)
    cache = FileCache(base_dir=tmp_path)

    rec = InstrumentRecord(
        instrument_id="EQ|AAPL",
        instrument_type="equity",
        provider="yfinance",
        provider_code="XNAS:AAPL",
        mic="XNAS",
        currency="USD",
        active_from=datetime(1900, 1, 1, tzinfo=timezone.utc),
        active_to=None,
        attrs={},
    )
    stores.catalog.upsert_instruments([rec])

    idx = pd.date_range("2024-01-01", periods=1, freq="D", tz=timezone.utc)
    df = pd.DataFrame(
        {"Open": [1.0], "High": [1.5], "Low": [0.5], "Close": [1.2], "Adj Close": [1.1], "Volume": [100]},
        index=idx,
    )

    fetch_and_store_yfinance(
        instrument_ids=["EQ|AAPL"],
        start=idx[0],
        end=idx[0],
        cfg=cfg,
        stores=stores,
        cache=cache,
        ttl=timedelta(days=1),
        download_fn=lambda *args, **kwargs: df,
    )

    sid = stores.columnar.get_series_id(
        instrument_id="EQ|AAPL", field="close", step_us=86_400_000_000
    )
    assert sid is not None
    stores.close()


def test_fallback_to_available_provider_code(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    stores = StoreContainer.open(db_path)
    cfg = _cfg(tmp_path, db_path)
    cache = FileCache(base_dir=tmp_path)

    # Insert canonical instrument but provider mapping points elsewhere.
    canonical = InstrumentRecord(
        instrument_id="XNAS|AAPL",
        instrument_type="equity",
        provider="yfinance",
        provider_code="XNAS|AAPL",
        mic="XNAS",
        currency="USD",
        active_from=datetime(1900, 1, 1, tzinfo=timezone.utc),
        active_to=None,
        attrs={},
    )
    alternate = InstrumentRecord(
        instrument_id="EQ|AAPL",
        instrument_type="equity",
        provider="yfinance",
        provider_code="AAPL",
        mic="XNAS",
        currency="USD",
        active_from=datetime(1900, 1, 1, tzinfo=timezone.utc),
        active_to=None,
        attrs={},
    )
    # Only alternate entry provides provider_code AAPL.
    stores.catalog.upsert_instruments([canonical, alternate])
    cur = stores.catalog.conn.cursor()
    cur.execute(
        "DELETE FROM instrument_provider_map WHERE provider = ? AND instrument_id = ? AND provider_code = ?",
        ("yfinance", "XNAS|AAPL", "XNAS|AAPL"),
    )
    stores.catalog.conn.commit()

    idx = pd.date_range("2024-01-01", periods=1, freq="D", tz=timezone.utc)
    df = pd.DataFrame(
        {"Open": [1.0], "High": [1.5], "Low": [0.5], "Close": [1.2], "Adj Close": [1.1], "Volume": [100]},
        index=idx,
    )

    fetch_and_store_yfinance(
        instrument_ids=["XNAS|AAPL"],
        start=idx[0],
        end=idx[0],
        cfg=cfg,
        stores=stores,
        cache=cache,
        ttl=timedelta(days=1),
        download_fn=lambda *args, **kwargs: df,
    )

    sid = stores.columnar.get_series_id(
        instrument_id="XNAS|AAPL", field="close", step_us=86_400_000_000
    )
    assert sid is not None
    stores.close()
