from datetime import datetime, timezone, timedelta

import math
import pytest

import sqlite3

import profit.cache.columnar_store as colmod
from profit.cache import ColumnarSqliteStore, SliceCorruptionError, SeriesNotFoundError


# Tests assume writes flush immediately, so keep pending_limit=1 in this suite.
ColumnarSqliteStore.DEFAULT_PENDING_LIMIT = 1


DAY_US = 86_400_000_000


def _dt(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


def test_write_creates_and_overwrites_canonical_windows(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
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
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        checksum_enabled=True,
    )
    strict.write(sid, [(_dt(1970, 1, 1), 1.0)])  # protected by checksum
    strict.flush()
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
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store.write(sid, [(_dt(1970, 1, 1), 42.0)])
    store.flush()

    # Reopen to ensure WAL + connection caching don't hide on-disk state.
    reopened = ColumnarSqliteStore(db_path)
    vals = reopened.read_slice_values(sid, 0)
    assert vals[0] == 42.0


def test_window_size_edges(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=1,
    )
    store.write(sid, [(_dt(1970, 1, 1), 7.0)])
    store.flush()
    vals = store.read_slice_values(sid, 0)
    assert vals == [7.0]

    sid_big = store.create_series(
        instrument_id="MSFT",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=8,
    )
    store.write(sid_big, [(_dt(1970, 1, 8), 8.0)])
    store.flush()
    vals_big = store.read_slice_values(sid_big, 0)
    assert vals_big[7] == 8.0


def test_empty_write_noop(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store.write(sid, [])
    vals = store.read_slice_values(sid, 0)
    assert math.isnan(vals[0])


def test_read_empty_range_returns_empty(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
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
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        compression="none",
    )
    sid_zlib = store.create_series(
        instrument_id="MSFT",
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
            field="close",
            step_us=0,  # invalid
            grid_origin_ts_us=0,
            window_points=2,
        )
    with pytest.raises(ValueError):
        store.create_series(
            instrument_id="AAPL",
            field="close",
            step_us=DAY_US,
            grid_origin_ts_us=0,
            window_points=0,  # invalid
        )
    with pytest.raises(ValueError):
        store.create_series(
            instrument_id="AAPL",
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
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    with pytest.raises(sqlite3.IntegrityError):
        store.create_series(
            instrument_id="AAPL",
            field="close",
            step_us=DAY_US,
            grid_origin_ts_us=0,
            window_points=2,
        )


def test_offsets_negative_roundtrip(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
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
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    sid2 = store.create_series(
        instrument_id="MSFT",
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


def test_high_water_mark_roundtrip(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    assert store.get_series(sid).high_water_ts_us is None

    store.bump_high_water_ts_us(sid, 1_000_000)
    assert store.get_series(sid).high_water_ts_us == 1_000_000

    # Lower bumps should be ignored.
    store.bump_high_water_ts_us(sid, 500_000)
    assert store.get_series(sid).high_water_ts_us == 1_000_000

    store.close()
    reopened = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    assert reopened.get_series(sid).high_water_ts_us == 1_000_000


def test_include_sentinel_true_with_nan(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        provider_id="test",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        sentinel_f64=float("nan"),
    )
    store.write(sid, [(_dt(1970, 1, 1), float("nan"))])
    pts = store.read_points(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 1), include_sentinel=True)
    assert math.isnan(pts[0][1])


def test_get_series_id_missing_returns_none(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.get_series_id(
        instrument_id="MSFT",
        field="close",
        step_us=DAY_US,
        provider_id="test",
    )
    assert series_id is None


def test_get_or_create_series_returns_existing(tmp_path, monkeypatch):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    existing = store.create_series(
        instrument_id="MSFT",
        field="close",
        provider_id="test",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )

    def _fail(*args, **kwargs):
        raise sqlite3.IntegrityError("race")

    monkeypatch.setattr(store, "create_series", _fail)
    sid = store.get_or_create_series(
        instrument_id="MSFT",
        field="close",
        provider_id="test",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    assert sid == existing


def test_find_series_configs_filters(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid_close = store.create_series(
        instrument_id="XNAS|AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    sid_volume = store.create_series(
        instrument_id="XNAS|AAPL",
        field="volume",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store.create_series(
        instrument_id="XNAS|MSFT",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )

    close_matches = store.find_series_configs(instrument_id="XNAS|AAPL", field="close")
    assert len(close_matches) == 1
    assert close_matches[0].series_id == sid_close

    all_matches = store.find_series_configs(instrument_id="XNAS|AAPL")
    assert {cfg.series_id for cfg in all_matches} == {sid_close, sid_volume}


def test_create_series_records_provider_id(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    series_id = store.create_series(
        instrument_id="AAPL",
        field="close",
        provider_id="stooq",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    cur = store._conn.execute("SELECT provider_id FROM __col_series__ WHERE series_id=?", (series_id,))
    assert cur.fetchone()[0] == "stooq"


def test_mark_range_fetched_and_completeness(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="MSFT",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )

    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 4)
    assert store.is_range_complete(sid, start=start, end=end) is False

    store.mark_range_fetched(sid, start=start, end=end)
    assert store.is_range_complete(sid, start=start, end=end) is True


def test_unfetched_vs_fetched_empty_range(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="MSFT",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 4)
    assert store.is_range_complete(sid, start=start, end=end) is False

    store.mark_range_fetched(sid, start=start, end=end)
    assert store.is_range_complete(sid, start=start, end=end) is True

    pts_no_sentinel = store.read_points(sid, start=start, end=end, include_sentinel=False)
    assert pts_no_sentinel == []

    pts_with_sentinel = store.read_points(sid, start=start, end=end, include_sentinel=True)
    assert len(pts_with_sentinel) == 4
    # All should now be data sentinel (not unfetched marker), so treated as fetched-but-missing.
    assert all(math.isnan(v) for _, v in pts_with_sentinel)


def test_mixed_slice_unfetched_and_written(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    store.write(sid, [(_dt(1970, 1, 1), 1.0)])  # only first point
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 4)
    assert store.is_range_complete(sid, start=start, end=end) is False

    store.mark_range_fetched(sid, start=start, end=end)
    assert store.is_range_complete(sid, start=start, end=end) is True

    pts = store.read_points(sid, start=start, end=end, include_sentinel=False)
    assert len(pts) == 1
    assert pts[0] == (_dt(1970, 1, 1), 1.0)

    pts_all = store.read_points(sid, start=start, end=end, include_sentinel=True)
    assert len(pts_all) == 4
    assert pts_all[0][1] == 1.0
    assert all(math.isnan(v) for _, v in pts_all[1:])


def test_partial_slices_and_unrelated_slices(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,
        sentinel_f64=float("nan"),
    )
    # Write indices 0,1 and 3,4 (leaving 2 and 5 unfetched).
    store.write(
        sid,
        [
            (_dt(1970, 1, 1), 10.0),
            (_dt(1970, 1, 2), 11.0),
            (_dt(1970, 1, 4), 20.0),
            (_dt(1970, 1, 5), 21.0),
        ],
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 6)
    assert store.is_range_complete(sid, start=start, end=end) is False

    store.mark_range_fetched(sid, start=start, end=end)
    assert store.is_range_complete(sid, start=start, end=end) is True

    pts = store.read_points(sid, start=start, end=end, include_sentinel=False)
    assert [p[1] for p in pts] == [10.0, 11.0, 20.0, 21.0]

    # A slice beyond the covered range remains unfetched.
    far = store.is_range_complete(sid, start=_dt(1970, 1, 7), end=_dt(1970, 1, 7))
    assert far is False


def test_get_unfetched_ranges(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    # Write a few points; leave gaps.
    store.write(sid, [(_dt(1970, 1, 2), 1.0), (_dt(1970, 1, 4), 2.0)])
    missing = store.get_unfetched_ranges(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 6))
    # Expect gaps: [day1], [day3], [day5-6]
    assert [(a.date(), b.date()) for a, b in missing] == [
        (_dt(1970, 1, 1).date(), _dt(1970, 1, 1).date()),
        (_dt(1970, 1, 3).date(), _dt(1970, 1, 3).date()),
        (_dt(1970, 1, 5).date(), _dt(1970, 1, 6).date()),
    ]


def test_mark_fetched_preserves_written_values(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 4)
    store.write(sid, [(_dt(1970, 1, 2), 42.0)])
    store.mark_range_fetched(sid, start=start, end=end)

    pts = store.read_points(sid, start=start, end=end, include_sentinel=False)
    assert pts == [(_dt(1970, 1, 2), 42.0)]


def test_get_unfetched_ranges_idempotent_after_mark(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
        sentinel_f64=float("nan"),
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 3)
    gaps = store.get_unfetched_ranges(sid, start=start, end=end)
    assert gaps
    for g0, g1 in gaps:
        store.mark_range_fetched(sid, start=g0, end=g1)
    assert store.get_unfetched_ranges(sid, start=start, end=end) == []


def test_get_unfetched_ranges_on_empty_series(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="MSFT",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,
        sentinel_f64=float("nan"),
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 9)  # spans three slices
    gaps = store.get_unfetched_ranges(sid, start=start, end=end)
    assert gaps == [(start, end)]
    assert store.is_range_complete(sid, start=start, end=end) is False


def test_mark_partial_window_on_empty_series(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="MSFT",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    full_start = _dt(1970, 1, 1)
    full_end = _dt(1970, 1, 8)
    # Mark only a sub-window as fetched.
    partial_start = _dt(1970, 1, 3)
    partial_end = _dt(1970, 1, 5)
    store.mark_range_fetched(sid, start=partial_start, end=partial_end)

    gaps = store.get_unfetched_ranges(sid, start=full_start, end=full_end)
    # Expect two gaps: before partial and after partial.
    assert [(a.date(), b.date()) for a, b in gaps] == [
        (_dt(1970, 1, 1).date(), _dt(1970, 1, 2).date()),
        (_dt(1970, 1, 6).date(), _dt(1970, 1, 8).date()),
    ]
    assert store.is_range_complete(sid, start=full_start, end=full_end) is False


def test_write_after_mark_fetched_keeps_complete(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 4)
    store.mark_range_fetched(sid, start=start, end=end)
    assert store.is_range_complete(sid, start=start, end=end) is True

    store.write(sid, [(_dt(1970, 1, 2), 5.0)])
    assert store.is_range_complete(sid, start=start, end=end) is True
    pts = store.read_points(sid, start=start, end=end, include_sentinel=False)
    assert pts == [(_dt(1970, 1, 2), 5.0)]


def test_offsets_enabled_empty_series(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="EURUSD",
        field="rate",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=3,
        sentinel_f64=float("nan"),
        offsets_enabled=True,
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 3)
    assert store.is_range_complete(sid, start=start, end=end) is False
    store.mark_range_fetched(sid, start=start, end=end)
    assert store.is_range_complete(sid, start=start, end=end) is True
    pts = store.read_points(sid, start=start, end=end, include_sentinel=False)
    assert pts == []


def test_non_daily_step_empty_series(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    hour_us = 3_600_000_000
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=hour_us,
        grid_origin_ts_us=0,
        window_points=6,  # 6-hour slices
        sentinel_f64=float("nan"),
    )
    start = datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(1970, 1, 1, 23, 0, tzinfo=timezone.utc)
    gaps = store.get_unfetched_ranges(sid, start=start, end=end)
    assert gaps[0][0] == start
    assert gaps[0][1] == end
    store.mark_range_fetched(sid, start=start, end=end)
    assert store.is_range_complete(sid, start=start, end=end) is True


def test_batched_reads_see_pending(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3", pending_limit=16)
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
    )
    pts = [(_dt(1970, 1, 1 + day), float(day)) for day in range(5)]
    store.write(sid, pts)
    vals = store.read_slice_values(sid, 0)
    assert vals[0] == 0.0
    assert vals[1] == 1.0

    # Verify that the points are pending by reading it from
    # a second connection.
    store2 = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    vals2 = store2.read_slice_values(sid, 0)
    assert math.isnan(vals2[0])
    assert math.isnan(vals2[1])


def test_batched_overwrite(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="GOOG",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
    )
    store.write(sid, [(_dt(1970, 1, 1 + day), float(day)) for day in range(4)])
    store.write(sid, [(_dt(1970, 1, 1 + day), float(day + 10)) for day in range(4)])
    vals = store.read_slice_values(sid, 0)
    assert vals[0] == 10.0
    assert vals[3] == 13.0


def test_close_flushes_pending(tmp_path):
    path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(path)
    sid = store.create_series(
        instrument_id="IBM",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
    )
    store.write(sid, [(_dt(1970, 1, 1 + day), float(day)) for day in range(2)])
    store.close()

    reopened = ColumnarSqliteStore(path)
    vals = reopened.read_slice_values(sid, 0)
    assert vals[0] == 0.0
    assert vals[1] == 1.0


def test_pending_dedup_option(tmp_path):
    store = ColumnarSqliteStore(
        tmp_path / "col.sqlite3",
        pending_limit=16,
        dedupe_pending=True,
    )
    sid = store.create_series(
        instrument_id="DUP",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
    )
    pts = [(_dt(1970, 1, 1 + day), float(day)) for day in range(4)]
    store.write(sid, pts)
    store.write(sid, [(_dt(1970, 1, 1 + day), float(day + 5)) for day in range(4)])
    assert len(store._pending_slices) == 1
    vals = store.read_slice_values(sid, 0)
    assert vals[0] == 5.0


def test_other_connection_sees_old_unflushed(tmp_path):
    path = tmp_path / "col.sqlite3"
    writer = ColumnarSqliteStore(path, pending_limit=16)
    sid = writer.create_series(
        instrument_id="TSLA",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
    )

    initial = [(_dt(1970, 1, day + 1), float(day)) for day in range(4)]
    writer.write(sid, initial)
    writer.flush()

    updated = [(_dt(1970, 1, day + 1), float(day + 10)) for day in range(4)]
    writer.write(sid, updated)

    reader = ColumnarSqliteStore(path)
    vals_before = reader.read_slice_values(sid, 0)
    assert vals_before[0] == 0.0
    assert vals_before[1] == 1.0

    writer.flush()
    reader.close()
    reopened = ColumnarSqliteStore(path)
    vals_after = reopened.read_slice_values(sid, 0)
    assert vals_after[0] == 10.0
    assert vals_after[1] == 11.0


def test_sparse_mark_leaves_holes(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    start = _dt(1970, 1, 1)
    end = _dt(1970, 1, 6)
    store.mark_range_fetched(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 2))
    store.mark_range_fetched(sid, start=_dt(1970, 1, 5), end=_dt(1970, 1, 5))
    gaps = store.get_unfetched_ranges(sid, start=start, end=end)
    assert [(a.date(), b.date()) for a, b in gaps] == [
        (_dt(1970, 1, 3).date(), _dt(1970, 1, 4).date()),
        (_dt(1970, 1, 6).date(), _dt(1970, 1, 6).date()),
    ]
    assert store.is_range_complete(sid, start=start, end=end) is False


def test_get_unfetched_ranges_multiple_slices(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    # Write only middle slice (indices 4-7)
    store.write(
        sid,
        [
            (_dt(1970, 1, 5), 10.0),
            (_dt(1970, 1, 6), 11.0),
        ],
    )
    missing = store.get_unfetched_ranges(sid, start=_dt(1970, 1, 1), end=_dt(1970, 1, 12))
    # Expect two gaps: first slice (0-3) and third slice (8-11)
    assert [(a.date(), b.date()) for a, b in missing] == [
        (_dt(1970, 1, 1).date(), _dt(1970, 1, 4).date()),
        (_dt(1970, 1, 7).date(), _dt(1970, 1, 12).date()),
    ]


def test_checkpoint_optimize_vacuum(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    # Should succeed even on empty DB.
    chk = store.checkpoint()
    assert len(chk) == 3
    store.optimize()
    store.vacuum()


def test_drop_series_removes_slices(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid = store.create_series(
        instrument_id="AAPL",
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    store.write(sid, [(_dt(1970, 1, 1), 1.0)])
    store.drop_series(sid)

    with pytest.raises(SeriesNotFoundError):
        store.read_slice_values(sid, 0)


def test_corrupt_offsets_checksum(tmp_path):
    db_path = tmp_path / "col.sqlite3"
    store = ColumnarSqliteStore(db_path)
    sid = store.create_series(
        instrument_id="AAPL",
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
        field="close",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=2,
    )
    with pytest.raises(ValueError, match="before grid origin"):
        store.write(sid, [(_dt(1969, 12, 31), 1.0)])
    with pytest.raises(ValueError, match="aligned"):
        store.read_slice_values(sid, 1)
