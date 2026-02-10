from __future__ import annotations

from typing import Any

from llm.stub_llm import StubLLM

from agent_v2.stages.initial_prompt import InitialPromptStage
from agentapi.plan import Run


def test_initial_prompt_emits_query_prior_insights_and_updates_user_context():
    backend = StubLLM({"STAGE: initial_prompt": '{"tags":["macro","rates"],"start_date":"2024-01-01","end_date":"2024-12-31"}'})
    stage = InitialPromptStage(question="What drove inflation in 2024?", backend=backend)

    user_context: dict[str, Any] = {}
    fragment = stage.run(previous_history_entries=[], user_context=user_context)
    assert isinstance(fragment, Run)
    assert fragment.stage_name == "query_prior_insights"

    assert user_context["question"] == "What drove inflation in 2024?"
    assert user_context["tags"] == ["macro", "rates"]
    assert user_context["start_date"] == "2024-01-01"
    assert user_context["end_date"] == "2024-12-31"
