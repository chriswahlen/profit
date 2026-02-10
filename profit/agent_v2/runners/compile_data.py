from __future__ import annotations

from pathlib import Path
from profit.agent_v2.runners.common import ContextualAgentRunner


def _read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


class CompileDataRunner(ContextualAgentRunner):
    """Stage: ask LLM to produce retrieval plan (Step 2)."""

    def __init__(self):
        # TODO: Implement
        raise Exception("aislop")

    def get_prompt(self, *, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")


    def process_prompt(self, *, result: str, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")

