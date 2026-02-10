from __future__ import annotations

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork

from agent_v2.stages.data_lookup_sec import DataLookupSECStage


def test_data_lookup_sec_placeholder():
    stage = DataLookupSECStage()
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
        "sec_requests": [{"key": "s1", "request": "AAPL capex 2023-2024", "why": "investment"}]
    }
    fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
    assert isinstance(fragment, Fork)
    assert "sec_datasets" in user_context
    assert user_context["sec_datasets"]["s1"]["kind"] == "sec_placeholder"
