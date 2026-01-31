from __future__ import annotations

from pathlib import Path

from profit.catalog.seeders import StooqUsEquitySeeder
from profit.catalog.store import CatalogStore


def test_stooq_us_seeder_registers_instrument(tmp_path: Path):
    catalog = CatalogStore(tmp_path / "catalog.sqlite3")

    sample = (
        tmp_path
        / "datasets"
        / "market"
        / "d_us_txt"
        / "data"
        / "daily"
        / "us"
        / "nyse stocks"
        / "1"
    )
    sample.mkdir(parents=True)
    (sample / "abc.us.txt").write_text("<TICKER>\n")

    seeder = StooqUsEquitySeeder(store=catalog, data_root=tmp_path, force=True)
    result = seeder.seed()
    assert result.instruments_written == 1

    instr = catalog.get_instrument("stooq", "ABC.US")
    assert instr is not None
    assert instr.instrument_id == "XNYS|ABC"
    assert instr.mic == "XNYS"
    assert instr.currency == "USD"
