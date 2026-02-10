from __future__ import annotations

from agentapi.plan import Run

from pathlib import Path

from profit.agent_v2.models import Answer
from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.runners.common import NoopLLMBackend


def _read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


class FinalResponseRunner(ContextualAgentRunner):
    """Terminal stage: craft final user response."""

    def __init__(self, *, backend, prompt_path: Path, question: str, answer_sink: dict):
        self.prompt_path = prompt_path
        self.question = question
        self._answer_sink = answer_sink
        super().__init__(name="final_response", backend=backend or NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        prompt = _read_prompt(self.prompt_path)
        if not prompt:
            prompt = "Provide a concise answer to the user's question."
        return prompt.replace("{{QUESTION}}", self.question)

    def process_prompt(self, *, result: str, previous_history_entries):
        text = result.strip()
        if not text:
            try:
                text = self.backend.generate(self.get_prompt(previous_history_entries=previous_history_entries))
            except Exception:
                text = "No answer generated."
        answer = Answer(text=text)
        self._answer_sink["answer"] = answer
        return Run(stage_name="final_response")
