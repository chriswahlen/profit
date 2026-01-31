from __future__ import annotations

from pathlib import Path

from profit.catalog.seeders import StooqDailySeeder
from profit.catalog.store import CatalogStore


def test_stooq_seeder_registers_instrument(tmp_path):
    catalog = CatalogStore(tmp_path / "catalog.sqlite3")

    sample = tmp_path / "market" / "d_world_txt" / "data" / "daily" / "world" / "currencies" / "major"
    sample.mkdir(parents=True)
    (sample / "usd.txt").write_text("<TICKER>\n")

    seeder = StooqDailySeeder(store=catalog, data_root=tmp_path, force=True)
    result = seeder.seed()
    assert result.instruments_written == 1

    instr = catalog.get_instrument("stooq", "USD")
    assert instr is not None
    assert instr.instrument_id.endswith("usd")
    assert instr.attrs["category"].startswith("world/")
