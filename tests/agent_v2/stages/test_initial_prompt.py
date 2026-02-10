from __future__ import annotations

from llm.stub_llm import StubLLM

from agent_v2.stages.initial_prompt import InitialPromptStage
from agentapi.plan import Run


def test_initial_prompt_emits_query_prior_insights_and_sets_metadata():
    backend = StubLLM({"STAGE: initial_prompt": '{"tags":["macro","rates"],"start_date":"2024-01-01","end_date":"2024-12-31"}'})
    stage = InitialPromptStage(question="What drove inflation in 2024?", backend=backend)

    fragment = stage.run(previous_history_entries=[])
    assert isinstance(fragment, Run)
    assert fragment.stage_name == "query_prior_insights"

    md = stage.history_metadata(fragment=fragment, previous_history_entries=[])
    assert md["question"] == "What drove inflation in 2024?"
    assert md["tags"] == ["macro", "rates"]
    assert md["start_date"] == "2024-01-01"
    assert md["end_date"] == "2024-12-31"
    assert md["user_context"]["question"] == "What drove inflation in 2024?"

