from __future__ import annotations

from typing import Any

import pytest

from agents.financial_advisor.skills.skill_interface import (
    SkillDescriptor,
    SkillExecutionResult,
    SkillInterface,
    SkillUsagePrompt,
)


class DummySkillInterface(SkillInterface):
    """
    Minimal implementation to make the SkillInterface testable.
    """

    def __init__(self) -> None:
        self._skills = [
            SkillDescriptor(
                skill_id="skill:edgar:filings",
                name="EDGAR filings",
                summary="Fetch filings metadata for a given CIK."
            )
        ]

    def list_skills(self):
        # Return a shallow copy so callers cannot mutate the source list.
        return list(self._skills)

    def describe_skill_usage(self, skill_id: str) -> SkillUsagePrompt:
        usage_mapping = {
            "skill:edgar:filings": SkillUsagePrompt(
                skill_id="skill:edgar:filings",
                prompt="Provide the latest filings for the given CIK and filing type.",
                example_questions=["What are the latest 10-Ks filed by CIK 0000320193?"]
            )
        }
        try:
            return usage_mapping[skill_id]
        except KeyError as exc:
            raise ValueError(f"Unknown skill_id {skill_id}") from exc

    def execute_skill(self, skill_id: str, payload: dict[str, Any]) -> SkillExecutionResult:
        if skill_id not in {self._skills[0].skill_id}:
            raise ValueError("Unknown skill_id")
        return SkillExecutionResult(skill_id=skill_id, records=[], metadata={"payload": payload})


def test_list_skills_returns_available_descriptors():
    skill_interface = DummySkillInterface()
    skills = skill_interface.list_skills()
    assert len(skills) == 1
    assert skills[0].skill_id == "skill:edgar:filings"
    assert skills[0].name == "EDGAR filings"


def test_describe_skill_usage_matches_known_skill():
    skill_interface = DummySkillInterface()
    usage = skill_interface.describe_skill_usage("skill:edgar:filings")
    assert usage.skill_id == "skill:edgar:filings"
    assert usage.prompt.startswith("Provide")
    assert usage.example_questions


def test_describe_skill_usage_unknown_skill_raises():
    skill_interface = DummySkillInterface()
    with pytest.raises(ValueError):
        skill_interface.describe_skill_usage("skill:unknown:foo")


def test_execute_skill_returns_metadata():
    skill_interface = DummySkillInterface()
    payload = {"question": "foo"}
    result = skill_interface.execute_skill("skill:edgar:filings", payload)
    assert isinstance(result, SkillExecutionResult)
    assert result.metadata["payload"] == payload


def test_execute_skill_unknown_skill_raises():
    skill_interface = DummySkillInterface()
    with pytest.raises(ValueError):
        skill_interface.execute_skill("skill:unknown", {})
