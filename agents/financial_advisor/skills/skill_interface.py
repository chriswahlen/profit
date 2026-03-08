from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence


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
