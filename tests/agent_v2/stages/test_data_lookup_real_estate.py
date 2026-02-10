from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork
from llm.stub_llm import StubLLM

from agent_v2.stages.data_lookup_real_estate import DataLookupRealEstateStage


def test_data_lookup_real_estate_returns_datasets():
    backend = StubLLM(
        {
            "STAGE: data_lookup_real_estate": '{"datasets":[{"key":"re1","table":"market_metrics","filters":{"region_id":"metro|us|live"},"rows":[{"period_start_date":"2024-01-01","median_sale_price":305000}],"query":"select median_sale_price from market_metrics"}]}'
        }
    )
    stage = DataLookupRealEstateStage(backend=backend)
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
        "real_estate_requests": [{"key": "re1", "request": "housing prices 2024", "why": "context"}],
    }
    fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
    assert isinstance(fragment, Fork)
    assert "real_estate_datasets" in user_context
    dataset = user_context["real_estate_datasets"]["re1"]
    assert dataset["table"] == "market_metrics"
    assert dataset["rows"][0]["median_sale_price"] == 305000
