from __future__ import annotations

import json
from pathlib import Path

from agentapi.plan import Fork

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.models import Answer


def _read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


class FinalResponseRunner(ContextualAgentRunner):
    """Terminal stage: craft final user response."""

    def __init__(self, *, backend, prompt_path: Path, question: str, answer_sink: dict):
        self.prompt_template = _read_prompt(prompt_path)
        self.question = question
        self.answer_sink = answer_sink
        super().__init__(name="final_response", backend=backend)

    def get_prompt(self, *, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        if "final_answer" in meta:
            # We already have a final answer; no need to hit LLM.
            return ""
        prompt = self.prompt_template or "Answer the question using provided data.\nQuestion: {{question}}\nData: {{data_payloads}}"
        prompt = prompt.replace("{{question}}", self.question)
        prompt = prompt.replace("{{step1_json}}", json.dumps(meta.get("step1", {}), ensure_ascii=False, indent=2))
        prompt = prompt.replace("{{step2_json}}", json.dumps(meta.get("step2", {}), ensure_ascii=False, indent=2))
        prompt = prompt.replace("{{insights}}", json.dumps(meta.get("prior_insights", []), ensure_ascii=False))
        prompt = prompt.replace("{{data_payloads}}", json.dumps(meta.get("data_payloads", []), ensure_ascii=False))
        prompt = prompt.replace("{{data_needs}}", json.dumps(meta.get("data_needs", []), ensure_ascii=False))
        return prompt.strip()

    def process_prompt(self, *, result: str, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        final_text = meta.get("final_answer") or result
        self.answer_sink["answer"] = Answer(text=final_text or "", step1=meta.get("step1"), step2=meta.get("step2"))
        self.set_meta(**meta, final_answer=final_text)
        # Terminal stage must not emit work; emit empty fork.
        return Fork(children=[])
