from datetime import datetime, timezone, timedelta

import math
import pytest

import sqlite3

import profit.cache.columnar_store as colmod
from profit.cache import ColumnarSqliteStore, SliceCorruptionError


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


def test_explicit_overwrite_same_slice(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )

    store.write(series_id, [(_dt(1970, 1, 1), 1.0)])
    store.write(series_id, [(_dt(1970, 1, 1), 2.5)])  # overwrite same point

    vals = store.read_slice_values(series_id, 0)
    assert vals[0] == 2.5


def test_offset_requires_millisecond_alignment(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="MSFT",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        offsets_enabled=True,
    )

    bad_ts = datetime(1970, 1, 2, 12, 0, 0, 123, tzinfo=timezone.utc)  # 123 microseconds
    with pytest.raises(ValueError, match="millisecond"):
        store.write(series_id, [(bad_ts, 5.0)])  # requires ms alignment when offsets enabled


def test_alignment_guard_on_read_slice(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
    )
    store.write(series_id, [(_dt(1970, 1, 1), 1.0)])

    with pytest.raises(ValueError):
        store.read_slice_values(series_id, 1)  # misaligned start_index


def test_checksum_catches_corruption(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    series_id = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        checksum_enabled=True,
    )
    store.write(series_id, [(_dt(1970, 1, 1), 1.0)])

    # Corrupt the values blob directly.
    con = sqlite3.connect(db_path)
    con.execute(
        "UPDATE __col_slice__ SET values_blob = x'00010203' WHERE series_id=? AND start_index=0",
        (series_id,),
    )
    con.commit()
    con.close()

    with pytest.raises(SliceCorruptionError):
        store.read_slice_values(series_id, 0)


def test_multi_slice_range_and_trimming(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,  # slices: [0,1,2], [3,4,5], ...
    )
    # Span two slices and read a sub-range to ensure trimming at boundaries.
    store.write(
        series_id,
        [
            (_dt(1970, 1, 1), 1.0),  # idx 0
            (_dt(1970, 1, 2), 2.0),  # idx 1
            (_dt(1970, 1, 3), 3.0),  # idx 2
            (_dt(1970, 1, 4), 4.0),  # idx 3
        ],
    )
    pts = store.read_points(
        series_id,
        start=_dt(1970, 1, 2),
        end=_dt(1970, 1, 4),
        include_sentinel=False,
    )
    assert [p[1] for p in pts] == [2.0, 3.0, 4.0]
    assert pts[0][0] == _dt(1970, 1, 2)
    assert pts[-1][0] == _dt(1970, 1, 4)


def test_sentinel_filtering_skips_only_sentinel(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        sentinel_f64=float("nan"),
    )
    # Make sure sentinel filtering does not drop legitimate zeros.
    store.write(
        series_id,
        [
            (_dt(1970, 1, 1), 0.0),  # legitimate zero
            (_dt(1970, 1, 2), float("nan")),
        ],
    )
    pts = store.read_points(series_id, start=_dt(1970, 1, 1), end=_dt(1970, 1, 2), include_sentinel=False)
    assert len(pts) == 1
    assert pts[0][1] == 0.0


def test_offsets_bounds(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="MSFT",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        offsets_enabled=True,
    )
    # Offset near midday should round-trip via offsets_ms.
    # Storing a midday event should rely on offsets, not a different bucket.
    ts = datetime(1970, 1, 2, 12, 0, tzinfo=timezone.utc)  # +12h offset
    store.write(series_id, [(ts, 5.0)])
    pts = store.read_points(series_id, start=_dt(1970, 1, 2), end=_dt(1970, 1, 2), include_sentinel=False)
    assert pts[0][0] == ts


def test_checksum_toggle(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    strict = ColumnarSqliteStore(db_path)
    sid = strict.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        checksum_enabled=True,
    )
    strict.write(sid, [(_dt(1970, 1, 1), 1.0)])  # protected by checksum
    con = sqlite3.connect(db_path)
    con.execute(
        "UPDATE __col_slice__ SET values_blob = x'00' WHERE series_id=? AND start_index=0",
        (sid,),
    )
    con.commit()
    con.close()
    with pytest.raises(SliceCorruptionError):
        strict.read_slice_values(sid, 0)

    lax = ColumnarSqliteStore(db_path)
    sid2 = lax.create_series(
        instrument_id="MSFT",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        checksum_enabled=False,
    )
    lax.write(sid2, [(_dt(1970, 1, 1), 2.0)])  # checksum disabled
    con = sqlite3.connect(db_path)
    # Corrupt one byte but preserve length to avoid decode error when checksums are disabled.
    cur = con.execute(
        "SELECT values_blob FROM __col_slice__ WHERE series_id=? AND start_index=0",
        (sid2,),
    )
    original = cur.fetchone()[0]
    mutated = bytes([original[0] ^ 0xFF]) + original[1:]
    con.execute(
        "UPDATE __col_slice__ SET values_blob = ? WHERE series_id=? AND start_index=0",
        (sqlite3.Binary(mutated), sid2),
    )
    con.commit()
    con.close()
    vals = lax.read_slice_values(sid2, 0)
    assert len(vals) == 2


def test_reopen_and_read(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store.write(sid, [(_dt(1970, 1, 1), 42.0)])

    # Reopen to ensure WAL + connection caching don't hide on-disk state.
    reopened = ColumnarSqliteStore(db_path)
    vals = reopened.read_slice_values(sid, 0)
    assert vals[0] == 42.0


def test_window_size_edges(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=1,
    )
    store.write(sid, [(_dt(1970, 1, 1), 7.0)])
    vals = store.read_slice_values(sid, 0)
    assert vals == [7.0]

    sid_big = store.create_series(
        instrument_id="MSFT",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=8,
    )
    store.write(sid_big, [(_dt(1970, 1, 8), 8.0)])
    vals_big = store.read_slice_values(sid_big, 0)
    assert vals_big[7] == 8.0


def test_empty_write_noop(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store.write(sid, [])
    with pytest.raises(KeyError):
        store.read_slice_values(sid, 0)


def test_read_empty_range_returns_empty(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    pts = store.read_points(
        sid,
        start=_dt(1970, 1, 1),
        end=_dt(1970, 1, 3),
        include_sentinel=False,
    )
    assert pts == []


def test_large_range_count(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=5,
        sentinel_f64=float("nan"),
    )
    # Write 10 consecutive days.
    store.write(
        sid,
        [(datetime(1970, 1, 1 + i, tzinfo=timezone.utc), float(i)) for i in range(10)],
    )
    pts = store.read_points(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 10), include_sentinel=True)
    assert len(pts) == 10
    assert pts[0][1] == 0.0 and pts[-1][1] == 9.0


def test_non_zero_origin_indexing(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    origin = DAY_US * 10  # origin at day 10
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=origin,
        window_points=2,
    )
    # Before origin should raise.
    with pytest.raises(ValueError):
        store.write(sid, [(_dt(1970, 1, 1), 1.0)])
    # After origin works.
    day10 = _dt(1970, 1, 11)  # 10 days after epoch (0-based)
    store.write(sid, [(day10, 10.0)])
    vals = store.read_slice_values(sid, 0)
    assert vals[0] == 10.0


def test_non_daily_step(tmp_path):
    hour_us = 3_600_000_000
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=hour_us,
        grid_origin_ts_us=0,
        window_points=6,  # 6-hour slice
    )
    # 5th hour should land at index 5 within the first 6-hour slice.
    ts = datetime(1970, 1, 1, 5, 0, tzinfo=timezone.utc)
    store.write(sid, [(ts, 5.0)])
    vals = store.read_slice_values(sid, 0)
    assert vals[5] == 5.0


def test_offset_and_compression_roundtrip(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,
        offsets_enabled=True,
        compression="zlib",
    )
    ts = datetime(1970, 1, 2, 15, 0, tzinfo=timezone.utc)
    store.write(sid, [(ts, 15.0)])
    pts = store.read_points(sid, start=_dt(1970, 1, 2), end=_dt(1970, 1, 2), include_sentinel=True)
    assert pts[0][0] == ts and pts[0][1] == 15.0


def test_sentinel_overwrite_multi_point(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,
        sentinel_f64=float("nan"),
    )
    store.write(
        sid,
        [
            (_dt(1970, 1, 1), 1.0),
            (_dt(1970, 1, 2), 2.0),
            (_dt(1970, 1, 3), 3.0),
        ],
    )
    store.write(
        sid,
        [
            (_dt(1970, 1, 2), float("nan")),
            (_dt(1970, 1, 3), 33.0),
        ],
    )
    vals = store.read_slice_values(sid, 0)
    assert math.isnan(vals[1])
    assert vals[2] == 33.0


def test_index_overflow_guard(monkeypatch, tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )

    # Force _dt_to_us to return a ts_us large enough to overflow index guard.
    def fake_dt_to_us(_):
        return (2**63) * DAY_US * 2

    monkeypatch.setattr(colmod, "_dt_to_us", fake_dt_to_us)
    with pytest.raises(ValueError):
        store.write(sid, [(_dt(1970, 1, 1), 1.0)])


def test_mixed_compression_coexistence(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    sid_plain = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        compression="none",
    )
    sid_zlib = store.create_series(
        instrument_id="MSFT",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        compression="zlib",
    )
    store.write(sid_plain, [(_dt(1970, 1, 1), 1.0)])
    store.write(sid_zlib, [(_dt(1970, 1, 1), 2.0)])

    assert store.read_slice_values(sid_plain, 0)[0] == 1.0
    assert store.read_slice_values(sid_zlib, 0)[0] == 2.0


def test_boundary_aligned_writes(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,  # boundaries at 0,3,6...
    )
    # Point exactly at boundary index 3 should land in second slice.
    store.write(sid, [(_dt(1970, 1, 4), 4.0)])
    # First slice was never created; read with require_existing=False via read_points.
    pts = store.read_points(sid, start=_dt(1970, 1, 4), end=_dt(1970, 1, 4))
    assert len(pts) == 1 and pts[0][1] == 4.0


def test_offsets_with_sentinel_filtering(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        offsets_enabled=True,
        sentinel_f64=float("nan"),
    )
    ts_real = datetime(1970, 1, 2, 12, 0, tzinfo=timezone.utc)
    store.write(
        sid,
        [
            (_dt(1970, 1, 1), float("nan")),  # sentinel
            (ts_real, 99.0),
        ],
    )
    pts = store.read_points(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 2), include_sentinel=False)
    assert len(pts) == 1
    assert pts[0][0] == ts_real and pts[0][1] == 99.0


def test_offsets_overflow_guard(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        offsets_enabled=True,
    )
    # Offset > int32 ms should fail. Construct a timestamp just beyond 24h within the same bucket.
    ts = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(days=1, milliseconds=2147483648 / 1000)
    with pytest.raises(ValueError):
        store.write(sid, [(ts, 1.0)])


def test_schema_invariants_on_create(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    with pytest.raises(ValueError):
        store.create_series(
            instrument_id="AAPL",
            dataset="bar_ohlcv",
            field="close",
            step_us=0,  # invalid
            grid_origin_ts_us=0,
            window_points=2,
        )
    with pytest.raises(ValueError):
        store.create_series(
            instrument_id="AAPL",
            dataset="bar_ohlcv",
            field="close",
            step_us=DAY_US,
            grid_origin_ts_us=0,
            window_points=0,  # invalid
        )
    with pytest.raises(ValueError):
        store.create_series(
            instrument_id="AAPL",
            dataset="bar_ohlcv",
            field="close",
            step_us=DAY_US,
            grid_origin_ts_us=0,
            window_points=2,
            compression="brotli",  # invalid
        )


def test_gap_in_range_does_not_emit_points(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,
        sentinel_f64=float("nan"),
    )
    # Only write first day; leave a gap for the rest of the range.
    store.write(sid, [(_dt(1970, 1, 1), 1.0)])
    pts = store.read_points(
        sid,
        start=_dt(1970, 1, 1),
        end=_dt(1970, 1, 3),
        include_sentinel=False,
    )
    assert len(pts) == 1
    assert pts[0][1] == 1.0


def test_concurrent_writers_same_db(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store1 = ColumnarSqliteStore(db_path)
    store2 = ColumnarSqliteStore(db_path)
    sid = store1.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store1.write(sid, [(_dt(1970, 1, 1), 1.0)])
    store2.write(sid, [(_dt(1970, 1, 2), 2.0)])
    pts = store1.read_points(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 2), include_sentinel=False)
    assert [p[1] for p in pts] == [1.0, 2.0]


def test_unique_series_constraint(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    with pytest.raises(sqlite3.IntegrityError):
        store.create_series(
            instrument_id="AAPL",
            dataset="bar_ohlcv",
            field="close",
            step_us=DAY_US,
            grid_origin_ts_us=0,
            window_points=2,
        )


def test_offsets_negative_roundtrip(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        offsets_enabled=True,
    )
    ts = datetime(1970, 1, 2, 0, 0, tzinfo=timezone.utc) - timedelta(hours=3)  # -3h relative to nominal day bucket
    store.write(sid, [(ts, 9.0)])
    pts = store.read_points(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 2), include_sentinel=False)
    assert pts[0][0] == ts and pts[0][1] == 9.0


def test_checksum_catches_offset_corruption(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        offsets_enabled=True,
        checksum_enabled=True,
    )
    store.write(sid, [(_dt(1970, 1, 2, 12, 0), 5.0)])
    con = sqlite3.connect(db_path)
    cur = con.execute("SELECT offsets_blob FROM __col_slice__ WHERE series_id=? AND start_index=0", (sid,))
    original = cur.fetchone()[0]
    mutated = bytes([original[0] ^ 0xAA]) + original[1:]
    con.execute(
        "UPDATE __col_slice__ SET offsets_blob = ? WHERE series_id=? AND start_index=0",
        (sqlite3.Binary(mutated), sid),
    )
    con.commit()
    con.close()
    with pytest.raises(SliceCorruptionError):
        store.read_slice_values(sid, 0)


def test_large_window_with_compression(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=512,
        compression="zlib",
    )
    store.write(sid, [(_dt(1970, 1, 1 + i), float(i)) for i in range(10)])
    vals = store.read_slice_values(sid, 0)
    assert vals[0] == 0.0 and vals[9] == 9.0


def test_cross_series_isolation(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid1 = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    sid2 = store.create_series(
        instrument_id="MSFT",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store.write(sid1, [(_dt(1970, 1, 1), 1.0)])
    store.write(sid2, [(_dt(1970, 1, 1), 2.0)])
    assert store.read_slice_values(sid1, 0)[0] == 1.0
    assert store.read_slice_values(sid2, 0)[0] == 2.0


def test_checksum_disabled_truncated_blob_raises_decode(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        checksum_enabled=False,
    )
    store.write(sid, [(_dt(1970, 1, 1), 1.0)])
    con = sqlite3.connect(db_path)
    con.execute(
        "UPDATE __col_slice__ SET values_blob = x'00' WHERE series_id=? AND start_index=0",
        (sid,),
    )
    con.commit()
    con.close()
    with pytest.raises(ValueError):
        store.read_slice_values(sid, 0)


def test_include_sentinel_true_with_nan(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        sentinel_f64=float("nan"),
    )
    store.write(sid, [(_dt(1970, 1, 1), float("nan"))])
    pts = store.read_points(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 1), include_sentinel=True)
    assert math.isnan(pts[0][1])


def test_corrupt_offsets_checksum(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        offsets_enabled=True,
        checksum_enabled=True,
    )
    store.write(sid, [(_dt(1970, 1, 1, 12, 0), 1.0)])
    con = sqlite3.connect(db_path)
    cur = con.execute(
        "SELECT offsets_blob FROM __col_slice__ WHERE series_id=? AND start_index=0",
        (sid,),
    )
    original = cur.fetchone()[0]
    mutated = bytes([original[0] ^ 0xFF]) + original[1:]
    con.execute(
        "UPDATE __col_slice__ SET offsets_blob = ? WHERE series_id=? AND start_index=0",
        (sqlite3.Binary(mutated), sid),
    )
    con.commit()
    con.close()
    with pytest.raises(SliceCorruptionError):
        store.read_slice_values(sid, 0)


def test_error_messages_stable(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        dataset="bar_ohlcv",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    with pytest.raises(ValueError, match="before grid origin"):
        store.write(sid, [(_dt(1969, 12, 31), 1.0)])
    with pytest.raises(ValueError, match="aligned"):
        store.read_slice_values(sid, 1)
