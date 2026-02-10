from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Iterable

from agentapi.runners import AgentTransformRunner
from llm.llm_backend import LLMBackend


@dataclass
class ContextualAgentRunner(AgentTransformRunner):
    """AgentTransformRunner that carries arbitrary metadata for downstream stages."""

    backend: LLMBackend
    name: str
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(name=self.name, backend=self.backend)

    def set_meta(self, **kwargs: Any) -> None:
        self.meta.update(kwargs)

    def history_metadata(self, fragment, previous_history_entries):  # noqa: D401
        return dict(self.meta)


class NoopLLMBackend(LLMBackend):
    """Minimal backend for tests; returns empty content."""

    def generate(self, prompt: str, run_id: str | None = None) -> str:  # type: ignore[override]
        return ""

    def generate_streaming(self, prompt: str, run_id: str | None = None) -> Iterable[str]:  # type: ignore[override]
        return []


__all__ = ["ContextualAgentRunner", "NoopLLMBackend"]
