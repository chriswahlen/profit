from __future__ import annotations

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork

from agent_v2.stages.data_lookup_real_estate import DataLookupRealEstateStage


def test_data_lookup_real_estate_placeholder():
    stage = DataLookupRealEstateStage()
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
        "real_estate_requests": [
            {"key": "re1", "request": "SF median sale price 2024", "why": "housing"}
        ]
    }
    fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
    assert isinstance(fragment, Fork)
    assert "real_estate_datasets" in user_context
    assert user_context["real_estate_datasets"]["re1"]["kind"] == "real_estate_placeholder"
