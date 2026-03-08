from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Sequence


@dataclass(frozen=True)
class SkillDescriptor:
    """
    Lightweight summary of a skill so callers can show what is available.
    """

    skill_id: str
    name: str
    summary: str


@dataclass(frozen=True)
class SkillUsagePrompt:
    """
    Richer information about how an agent should invoke a particular skill.
    """

    skill_id: str
    prompt: str
    example_questions: Sequence[str]


@dataclass(frozen=True)
class SkillExecutionResult:
    """
    Structured result returned after a skill has executed.
    """

    skill_id: str
    records: Sequence[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)


class SkillInterface(ABC):
    """
    Contract that data sources can implement to expose their agent-facing skills.
    """

    @abstractmethod
    def list_skills(self) -> Sequence[SkillDescriptor]:
        """
        Return brief metadata for each skill this data source exposes.
        """
        raise NotImplementedError("must be implemented by subclasses")

    @abstractmethod
    def describe_skill_usage(self, skill_id: str) -> SkillUsagePrompt:
        """
        Provide the prompt/instructions that agents should follow when invoking the
        skill identified by `skill_id`.
        """
        raise NotImplementedError("must be implemented by subclasses")

    @abstractmethod
    def execute_skill(self, skill_id: str, payload: dict[str, Any]) -> SkillExecutionResult:
        """
        Run the requested skill using the provided JSON payload and return the result.
        """
        raise NotImplementedError("must be implemented by subclasses")
