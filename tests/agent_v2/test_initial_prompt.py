from __future__ import annotations

from agentapi.plan import Run

import pytest

from profit.agent_v2.exceptions import AgentV2ValidationError
from profit.agent_v2.runners.common import NoopLLMBackend
from profit.agent_v2.runners.initial_prompt import InitialPromptRunner


def _valid_result() -> str:
    return """{
        "approach": "Gather prior insights for valuation.",
        "insights": [
            {
                "tags": ["growth", "capital"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31"
            },
            {
                "tags": ["risk"]
            }
        ]
    }"""


def test_initial_prompt_stores_user_context():
    runner = InitialPromptRunner(
        backend=NoopLLMBackend(),
        question="What is the outlook on MSFT?",
        hints=[],
        extra_instructions=None,
    )

    result = runner.process_prompt(result=_valid_result(), previous_history_entries=[])
    assert isinstance(result, Run)
    assert result.stage_name == "query_prior_insights"

    context = runner.meta.get("user_context")
    assert context is not None
    assert context["approach"] == "Gather prior insights for valuation."
    assert context["insights"][0]["tags"] == ["growth", "capital"]
    assert context["insights"][0]["start_date"] == "2024-01-01"
    assert context["insights"][0]["end_date"] == "2024-12-31"
    assert context["insights"][1]["tags"] == ["risk"]


def test_initial_prompt_rejects_bad_json():
    runner = InitialPromptRunner(
        backend=NoopLLMBackend(),
        question="Bad input",
        hints=[],
        extra_instructions=None,
    )

    bad_response = '{"insights": []}'
    with pytest.raises(AgentV2ValidationError):
        runner.process_prompt(result=bad_response, previous_history_entries=[])
