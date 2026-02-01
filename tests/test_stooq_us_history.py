from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.seeders import StooqUsHistorySeeder


def test_stooq_us_history_seeder(tmp_path: Path):
    # Arrange sample file
    zip_path = tmp_path / "datasets" / "stooq" / "d_us_txt.zip"
    zip_path.parent.mkdir(parents=True)
    import zipfile, io
    content = "\n".join(
        [
            "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>",
            "AAPL.US,D,20240102,000000,100,110,90,105,12345,0",
        ]
    ) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data/daily/us/nasdaq stocks/1/aapl.us.txt", content)

    col_store = ColumnarSqliteStore(tmp_path / "profit.sqlite")
    seeder = StooqUsHistorySeeder(store=col_store, data_root=tmp_path, force=True)

    # Act
    result = seeder.seed()

    # Assert
    # 6 fields per bar -> 6 points written
    assert result.rows_written == 6
    # close series should hold the point
    series_id = col_store.get_series_id(
        instrument_id="XNAS|AAPL",
        field="close",
        step_us=86_400_000_000,
        provider_id="stooq",
    )
    assert series_id is not None
    points = col_store.read_points(
        series_id,
        start=datetime(2024, 1, 2, tzinfo=timezone.utc),
        end=datetime(2024, 1, 2, tzinfo=timezone.utc),
        include_sentinel=False,
    )
    # only one point, value 105 close
    assert len(points) == 1
    assert points[0][1] == 105.0
