from datetime import datetime, timezone
from pathlib import Path

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.seeders import StooqWorldHistorySeeder


def test_stooq_world_history_seeder(tmp_path: Path):
    base = (
        tmp_path
        / "datasets"
        / "market"
        / "d_world_txt"
        / "data"
        / "daily"
        / "world"
        / "stooq stocks indices"
        / "1"
    )
    base.mkdir(parents=True)
    sample = base / "aapl.us.txt"
    sample.write_text(
        "\n".join(
            [
                "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>",
                "AAPL.US,D,20240102,000000,100,110,90,105,9999,0",
            ]
        )
        + "\n"
    )

    col_store = ColumnarSqliteStore(tmp_path / "profit.sqlite")
    seeder = StooqWorldHistorySeeder(store=col_store, data_root=tmp_path, force=True)
    result = seeder.seed()
    assert result.rows_written == 6

    series_id = col_store.get_series_id(
        instrument_id="XNAS|AAPL",
        dataset="stooq_world_bar_ohlcv",
        field="close",
        step_us=86_400_000_000,
    )
    assert series_id is not None
    pts = col_store.read_points(
        series_id,
        start=datetime(2024, 1, 2, tzinfo=timezone.utc),
        end=datetime(2024, 1, 2, tzinfo=timezone.utc),
        include_sentinel=False,
    )
    assert len(pts) == 1
    assert pts[0][1] == 105.0
