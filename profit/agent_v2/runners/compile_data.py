from __future__ import annotations

import json
from pathlib import Path

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.validation import parse_step2
from profit.agent_v2.exceptions import AgentV2RuntimeError


def _read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


class CompileDataRunner(ContextualAgentRunner):
    """Stage: ask LLM to produce retrieval plan (Step 2)."""

    def __init__(self, *, backend, compiler_path: Path, question: str):
        self.prompt_template = _read_prompt(compiler_path)
        self.question = question
        super().__init__(name="compile_data", backend=backend)

    def get_prompt(self, *, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        step1 = meta.get("step1", {})
        prior_insights = meta.get("prior_insights", [])
        prompt = (self.prompt_template or "Given anchors, produce retrieval plan for: {{question}}")
        prompt = prompt.replace("{{question}}", self.question)
        prompt = prompt.replace("{{step1_json}}", json.dumps(step1, ensure_ascii=False, indent=2))
        prompt = prompt.replace("{{prior_insights}}", json.dumps(prior_insights, ensure_ascii=False, indent=2))
        return prompt.strip()

    def process_prompt(self, *, result: str, previous_history_entries):
        meta = previous_history_entries[0].metadata if previous_history_entries else {}
        step1 = meta.get("step1", {})
        try:
            step2 = parse_step2(result)
            self.set_meta(step1=step1, step2=step2.raw, prior_insights=meta.get("prior_insights", []))
            return Run(stage_name="data_lookup_market")
        except Exception:
            # treat as final answer
            self.set_meta(step1=step1, prior_insights=meta.get("prior_insights", []), final_answer=result)
            return Run(stage_name="final_response")
