from __future__ import annotations

from pathlib import Path

from profit.catalog.seeders import StooqDailySeeder
from profit.catalog.store import CatalogStore


def test_stooq_seeder_registers_instrument(tmp_path):
    catalog = CatalogStore(tmp_path / "catalog.sqlite3")

    zip_path = tmp_path / "datasets" / "stooq" / "d_world_txt.zip"
    zip_path.parent.mkdir(parents=True)
    import zipfile
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data/daily/world/currencies/major/usd.txt", "<TICKER>\n")

    seeder = StooqDailySeeder(store=catalog, data_root=tmp_path, force=True)
    result = seeder.seed()
    assert result.instruments_written == 1

    instr = catalog.get_instrument("stooq", "USD")
    assert instr is not None
    assert instr.instrument_id == "FX|USD"
    assert instr.attrs["category"].startswith("world/")
