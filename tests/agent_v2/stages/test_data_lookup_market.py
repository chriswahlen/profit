from __future__ import annotations

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork

from agent_v2.stages.data_lookup_market import DataLookupMarketStage


def test_data_lookup_market_returns_datasets():
    stage = DataLookupMarketStage()
    parent = HistoryEntry(
        run_id="run_parent",
        parent_run_ids=[],
        logical_invocation_id="run_parent",
        attempt_number=1,
        stage_name="query_prior_insights",
        status="succeeded",
        timestamp=0.0,
        result="ok",
        metadata={},
    )
    user_context = {
        "market_requests": [{"key": "m1", "request": "S&P 500 close 2024", "why": "baseline"}]
    }
    fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
    assert isinstance(fragment, Fork)
    assert "market_datasets" in user_context
    assert "m1" in user_context["market_datasets"]
    assert user_context["market_datasets"]["m1"]["kind"] == "synthetic_daily_series"
