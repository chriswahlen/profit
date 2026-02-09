from __future__ import annotations

import json
from pathlib import Path

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.validation import parse_step1
from profit.agent_v2.models import Step1Result


def _read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


class InitialPromptRunner(ContextualAgentRunner):
    """Stage: run planner prompt, emit query_prior_insights."""

    def __init__(self, *, backend, planner_path: Path, question: str, hints: list[str], extra_instructions: str | None):
        self.prompt_template = _read_prompt(planner_path)
        self.question = question
        self.hints = hints
        self.extra_instructions = extra_instructions
        super().__init__(name="initial_prompt", backend=backend)

    def get_prompt(self, *, previous_history_entries):
        template = self.prompt_template or "You are a finance research agent. Answer: {{question}}"
        hint_block = "\n".join(f"- {h}" for h in self.hints if h)
        extra = f"\nAdditional instructions:\n{self.extra_instructions}" if self.extra_instructions else ""
        prompt = template.replace("{{question}}", self.question)
        prompt = prompt.replace("{{hints}}", hint_block)
        prompt = prompt + extra
        return prompt.strip()

    def process_prompt(self, *, result: str, previous_history_entries):
        step1: Step1Result = parse_step1(result)
        self.set_meta(step1=step1.raw)
        if step1.can_answer_now and step1.final_answer:
            self.set_meta(final_answer=step1.final_answer)
            return Run(stage_name="final_response")
        return Run(stage_name="query_prior_insights")
