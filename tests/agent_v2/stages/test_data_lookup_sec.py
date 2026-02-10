from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork
from llm.stub_llm import StubLLM

from agent_v2.stages.data_lookup_sec import DataLookupSECStage


def test_data_lookup_sec_returns_datasets():
    backend = StubLLM(
        {
            "STAGE: data_lookup_sec": (
                '{"datasets":[{"key":"s1","table":"xbrl_fact","filters":{"accession":"0001"},"rows":[{"concept_id":1,"value_numeric":123.45}],"query":"facts for capex"}]}'
            )
        }
    )
    stage = DataLookupSECStage(backend=backend)
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
        "sec_requests": [{"key": "s1", "request": "capex summary", "why": "analysis"}],
    }
    fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
    assert isinstance(fragment, Fork)
    assert "sec_datasets" in user_context
    dataset = user_context["sec_datasets"]["s1"]
    assert dataset["table"] == "xbrl_fact"
    assert dataset["rows"][0]["value_numeric"] == 123.45
