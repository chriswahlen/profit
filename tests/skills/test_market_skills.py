from __future__ import annotations

import pytest

from agents.financial_advisor.skills.market_skills import MarketSkills
from config import Config
from data_sources.market.market_data_store import Candle, MarketDataStore


@pytest.fixture
def market_store(tmp_path, monkeypatch):
    monkeypatch.setenv("PROFIT_DATA_PATH", str(tmp_path))
    store = MarketDataStore(Config())
    try:
        yield store
    finally:
        store.close()


def _seed_market_data(store: MarketDataStore) -> None:
    candles = [
        Candle(
            canonical_id="sec:xnas:aapl",
            start_ts="2026-01-02",
            open=150.0,
            high=152.0,
            low=149.0,
            close=151.0,
            adj_close=151.0,
            volume=1000.0,
            provider="stooq",
        ),
        Candle(
            canonical_id="sec:xnas:aapl",
            start_ts="2026-01-30",
            open=152.0,
            high=155.0,
            low=151.0,
            close=154.0,
            adj_close=154.0,
            volume=1200.0,
            provider="stooq",
        ),
        Candle(
            canonical_id="sec:xnas:aapl",
            start_ts="2026-02-10",
            open=155.0,
            high=158.0,
            low=154.0,
            close=157.0,
            adj_close=157.0,
            volume=1100.0,
            provider="stooq",
        ),
    ]
    store.upsert_candles_raw(candles)


def test_skill_list_includes_quotes(market_store):
    skills = MarketSkills(market_store)
    listed = skills.list_skills()
    assert any(skill.skill_id == MarketSkills.SKILL_QUOTES for skill in listed)


def test_quote_skill_prompt_mentions_symbol(market_store):
    skills = MarketSkills(market_store)
    usage = skills.describe_skill_usage(MarketSkills.SKILL_QUOTES)
    assert "symbol" in usage.prompt
    assert "columns" in usage.prompt


def test_execute_quotes_returns_rows(market_store):
    _seed_market_data(market_store)
    skills = MarketSkills(market_store)
    payload = {
        "symbol": "sec:xnas:aapl",
        "columns": ["open", "close", "volume"],
        "start": "2026-01-01",
        "end": "2026-02-28",
    }
    result = skills.execute_skill(MarketSkills.SKILL_QUOTES, payload)
    assert result.metadata["row_count"] == 3
    assert all("open" in record for record in result.records)
    assert result.records[0]["start_ts"] >= result.records[-1]["start_ts"]


def test_execute_quotes_with_aggregation_produces_monthly_averages(market_store):
    _seed_market_data(market_store)
    skills = MarketSkills(market_store)
    payload = {
        "symbol": "sec:xnas:aapl",
        "columns": ["close", "volume"],
        "start": "2026-01-01",
        "end": "2026-02-28",
        "aggregation": {"method": "avg", "period": "month"},
    }
    result = skills.execute_skill(MarketSkills.SKILL_QUOTES, payload)
    assert result.metadata["aggregation"] == "avg"
    assert result.metadata["period"] == "month"
    assert result.metadata["aggregate_rows"] == 2
    assert all("period_label" in record for record in result.records)


def test_execute_quotes_invalid_column_raises(market_store):
    skills = MarketSkills(market_store)
    payload = {
        "symbol": "sec:xnas:aapl",
        "columns": ["bogus"],
    }
    with pytest.raises(ValueError):
        skills.execute_skill(MarketSkills.SKILL_QUOTES, payload)
