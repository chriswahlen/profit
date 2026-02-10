from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork
from llm.stub_llm import StubLLM

from agent_v2.stages.data_lookup_market import DataLookupMarketStage


def test_data_lookup_market_returns_datasets():
    backend = StubLLM(
        {
            "STAGE: data_lookup_market": (
                '{"datasets":[{"key":"m1","query":{"region_ids":["metro|us|live"],'
                '"start_date":"2024-01-01","end_date":"2024-01-07"},"rows":'
                '[{"region_id":"metro|us|live","period_start_date":"2024-01-01",'
                '"median_sale_price":550000}]}]}'
            )
        }
    )
    stage = DataLookupMarketStage(backend=backend)
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
    user_context: dict[str, Any] = {
        "question": "Q",
        "market_requests": [{"key": "m1", "request": "S&P 500 close 2024", "why": "baseline"}],
    }
    fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
    assert isinstance(fragment, Fork)
    assert "market_datasets" in user_context
    dataset = user_context["market_datasets"]["m1"]
    assert dataset["query"]["region_ids"] == ["metro|us|live"]
    assert dataset["rows"][0]["median_sale_price"] == 550000
