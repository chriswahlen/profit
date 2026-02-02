from datetime import date

from profit.agent import retrievers
from profit.agent.types import RetrievalPlan
from profit.stores.redfin_store import RedfinStore


def _seed_redfin(tmp_path):
    db_path = tmp_path / "redfin.sqlite"
    store = RedfinStore(db_path)
    cur = store.conn.cursor()
    cur.execute(
        "INSERT INTO regions(region_id, region_type, name, canonical_code, country_iso2) VALUES (?, ?, ?, ?, ?)",
        ("region|seattle", "city", "Seattle", "SEA", "US"),
    )
    cur.execute(
        """
        INSERT INTO market_metrics (
            region_id, period_start_date, period_granularity, data_revision, source_provider,
            median_sale_price, homes_sold
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("region|seattle", "2024-01-01", "week", 1, "redfin", 750000.0, 10),
    )
    store.conn.commit()
    return db_path


def test_fetch_redfin_reads_metrics(tmp_path):
    db_path = _seed_redfin(tmp_path)
    plan = RetrievalPlan(
        source="redfin",
        regions=("Seattle",),
        start=date(2024, 1, 1),
        end=date(2024, 1, 2),
    )
    result = retrievers.fetch(plan, redfin_db_path=db_path)
    payload = result.payload

    assert payload["provider"] == "redfin"
    region = payload["regions"][0]
    assert region["name"] == "Seattle"
    metrics = region["metrics"]
    assert metrics[0]["median_sale_price"] == 750000.0
    assert metrics[0]["homes_sold"] == 10
    assert payload["unresolved_regions"] == []
