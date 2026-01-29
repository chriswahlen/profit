from datetime import datetime, timezone

import math
import pytest

from profit.cache import ColumnarSqliteStore


DAY_US = 86_400_000_000


def _dt(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


def test_write_creates_and_overwrites_canonical_windows(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,  # unix epoch
        window_points=4,
        compression="none",
        offsets_enabled=False,
        checksum_enabled=True,
        sentinel_f64=float("nan"),
    )

    # Write two points within the same canonical window (days 0..3).
    store.write(
        series_id,
        [
            (_dt(1970, 1, 1), 10.0),
            (_dt(1970, 1, 2), 11.0),
        ],
    )
    vals = store.read_slice_values(series_id, 0)
    assert vals[0] == 10.0
    assert vals[1] == 11.0
    assert math.isnan(vals[2])
    assert math.isnan(vals[3])

    # Explicit sentinel overwrites should clear prior values.
    store.write(series_id, [(_dt(1970, 1, 2), float("nan"))])
    vals2 = store.read_slice_values(series_id, 0)
    assert vals2[0] == 10.0
    assert math.isnan(vals2[1])

    # A point in the next window (days 4..7) creates a new slice.
    store.write(series_id, [(_dt(1970, 1, 5), 20.0)])
    vals3 = store.read_slice_values(series_id, 4)
    assert vals3[0] == 20.0


def test_offsets_capture_actual_timestamp(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="MSFT",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        compression="zlib",
        offsets_enabled=True,
        checksum_enabled=True,
    )

    # Same day bucket, but a 12:00 timestamp should be captured via offset.
    store.write(series_id, [(_dt(1970, 1, 2, 12, 0), 123.0)])
    pts = store.read_points(series_id, start=_dt(1970, 1, 2), end=_dt(1970, 1, 2), include_sentinel=True)
    assert len(pts) == 1
    assert pts[0][0] == _dt(1970, 1, 2, 12, 0)
    assert pts[0][1] == 123.0


def test_write_before_origin_raises(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=DAY_US,  # origin is 1970-01-02
        window_points=4,
    )

    with pytest.raises(ValueError):
        store.write(series_id, [(_dt(1970, 1, 1), 1.0)])

