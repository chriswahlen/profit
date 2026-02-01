from datetime import datetime, timezone
from pathlib import Path

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.seeders import StooqWorldHistorySeeder


def test_stooq_world_history_seeder(tmp_path: Path):
    zip_path = tmp_path / "datasets" / "stooq" / "d_world_txt.zip"
    zip_path.parent.mkdir(parents=True)
    import zipfile
    content = "\n".join(
        [
            "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>",
            "AAPL.US,D,20240102,000000,100,110,90,105,9999,0",
        ]
    ) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data/daily/world/stooq stocks indices/1/aapl.us.txt", content)

    col_store = ColumnarSqliteStore(tmp_path / "profit.sqlite")
    seeder = StooqWorldHistorySeeder(store=col_store, data_root=tmp_path, force=True)
    result = seeder.seed()
    assert result.rows_written == 6

    series_id = col_store.get_series_id(
        instrument_id="XNAS|AAPL",
        field="close",
        step_us=86_400_000_000,
        provider_id="stooq",
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
