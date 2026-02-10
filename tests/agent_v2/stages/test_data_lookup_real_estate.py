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
        metadata={
            "question": "Q",
            "market_requests": [],
            "real_estate_requests": [{"key": "re1", "request": "SF median sale price 2024", "why": "housing"}],
            "sec_requests": [],
        },
    )
    fragment = stage.run(previous_history_entries=[parent])
    assert isinstance(fragment, Fork)
    md = stage.history_metadata(fragment=fragment, previous_history_entries=[parent])
    assert "real_estate_datasets" in md
    assert md["real_estate_datasets"]["re1"]["kind"] == "real_estate_placeholder"

