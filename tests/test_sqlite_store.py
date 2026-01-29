from datetime import datetime, timezone

import pytest

from profit.cache import DatasetNotFoundError, SchemaError, SqliteStore


def test_create_append_read_roundtrip(tmp_path):
    store = SqliteStore(tmp_path / "cache.sqlite3")
    schema = {
        "instrument_id": "TEXT",
        "ts_utc": "TIMESTAMP",
        "close": "REAL",
        "volume": "INTEGER",
    }
    store.create_dataset("bar_ohlcv", schema, primary_keys=["instrument_id", "ts_utc"])

    rows = [
        {
            "instrument_id": "AAPL",
            "ts_utc": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "close": 150.1,
            "volume": 1_000,
        },
        {
            "instrument_id": "AAPL",
            "ts_utc": datetime(2020, 1, 2, tzinfo=timezone.utc),
            "close": 151.2,
            "volume": 900,
        },
    ]

    inserted = store.append("bar_ohlcv", rows)
    assert inserted == len(rows)

    fetched = store.read("bar_ohlcv", order_by=["ts_utc"], as_dataframe=False)
    assert [r["volume"] for r in fetched] == [1_000, 900]
    assert all(r["ts_utc"].tzinfo == timezone.utc for r in fetched)


def test_column_projection_and_overwrite(tmp_path):
    store = SqliteStore(tmp_path / "cache.sqlite3")
    schema = {
        "instrument_id": "TEXT",
        "ts_utc": "TIMESTAMP",
        "close": "REAL",
        "volume": "INTEGER",
    }
    store.create_dataset("bar_ohlcv", schema, primary_keys=["instrument_id", "ts_utc"])

    row = {
        "instrument_id": "MSFT",
        "ts_utc": datetime(2020, 5, 1, tzinfo=timezone.utc),
        "close": 180.0,
        "volume": 500,
    }
    store.append("bar_ohlcv", [row])

    projected = store.read(
        "bar_ohlcv",
        columns=["ts_utc", "close"],
        where="instrument_id = :code",
        params={"code": "MSFT"},
        as_dataframe=False,
    )
    assert projected[0] == {
        "ts_utc": row["ts_utc"],
        "close": row["close"],
    }

    # Overwrite the existing row using primary key.
    store.append(
        "bar_ohlcv",
        [
            {
                **row,
                "volume": 999,
            }
        ],
        overwrite=True,
    )
    updated = store.read(
        "bar_ohlcv",
        where="instrument_id = :code",
        params={"code": "MSFT"},
        as_dataframe=False,
    )
    assert updated[0]["volume"] == 999


def test_infer_schema_on_create(tmp_path):
    store = SqliteStore(tmp_path / "cache.sqlite3")
    rows = [
        {
            "series_id": "FX:EURUSD",
            "ts_utc": datetime(2021, 7, 1, 12, 0, tzinfo=timezone.utc),
            "rate": 1.18,
            "is_spot": True,
        }
    ]
    store.append("fx_rate", rows, create=True)

    schema = store.get_schema("fx_rate")
    types = {c.name: c.declared_type for c in schema}
    assert types["ts_utc"] == "TIMESTAMP"
    assert types["is_spot"] == "INTEGER"

    fetched = store.read("fx_rate", as_dataframe=False)
    assert fetched[0]["is_spot"] is True


def test_unknown_dataset_raises(tmp_path):
    store = SqliteStore(tmp_path / "cache.sqlite3")
    with pytest.raises(DatasetNotFoundError):
        store.read("missing", as_dataframe=False)

    with pytest.raises(DatasetNotFoundError):
        store.append("missing", [{"a": 1}], create=False)
