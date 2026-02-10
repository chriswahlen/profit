from __future__ import annotations

from pathlib import Path
import json

from agentapi.plan import Run

from profit.agent_v2.models import Request, RetrievalBatch
from profit.agent_v2.runners.common import ContextualAgentRunner


def _read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


class CompileDataRunner(ContextualAgentRunner):
    """Stage: ask LLM to produce retrieval plan (Step 2)."""

    def __init__(self, *, backend, compiler_path: Path, question: str):
        self.compiler_path = compiler_path
        self.question = question
        super().__init__(name="compile_data", backend=backend)

    def get_prompt(self, *, previous_history_entries):
        # Use the provided prompt template, falling back to a minimal instruction.
        prompt = _read_prompt(self.compiler_path) or "Draft a data retrieval plan as JSON."
        return prompt.replace("{{QUESTION}}", self.question)

    def process_prompt(self, *, result: str, previous_history_entries):
        """Parse (or synthesize) a Step 2 retrieval plan."""
        try:
            payload = json.loads(result) if result.strip() else {}
        except json.JSONDecodeError:
            payload = {}

        batches: list[RetrievalBatch] = []
        for batch in payload.get("batches", []) if isinstance(payload, dict) else []:
            requests = []
            for req in batch.get("requests", []) if isinstance(batch, dict) else []:
                if not isinstance(req, dict):
                    continue
                requests.append(
                    Request(
                        request_id=req.get("request_id", "req_0"),
                        type=req.get("type", "unknown"),
                        params=req.get("params", {}),
                        dataset=req.get("dataset"),
                    )
                )
            batches.append(
                RetrievalBatch(
                    batch_id=batch.get("batch_id", "batch_0"),
                    purpose=batch.get("purpose", "unspecified"),
                    requests=requests,
                    depends_on_batches=batch.get("depends_on_batches", []) or [],
                )
            )

        # Skip straight to final response for now.
        return Run(stage_name="final_response")
