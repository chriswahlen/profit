from __future__ import annotations

from typing import Any

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Fork

from agent_v2.stages.final_response import FinalResponseStage


def test_final_response_terminal_sets_user_context():
    stage = FinalResponseStage()
    parent = HistoryEntry(
        run_id="run_parent",
        parent_run_ids=[],
        logical_invocation_id="run_parent",
        attempt_number=1,
        stage_name="compile_data",
        status="succeeded",
        timestamp=0.0,
        result="ok",
        metadata={},
    )
    user_context: dict[str, Any] = {"final_answer": "Hello."}
    fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
    assert isinstance(fragment, Fork)
    assert user_context["final_answer"] == "Hello."
