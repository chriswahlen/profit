from __future__ import annotations

from profit.agent_v2.runners.common import ContextualAgentRunner, NoopLLMBackend
from profit.agent_v2.retrievers import EdgarRetrieverV2
from agentapi.plan import Run
from llm.llm_backend import LLMBackend


class DataLookupSecRunner(ContextualAgentRunner):
    """Stage 3c: fetch SEC/EDGAR data."""

    def __init__(self, *, backend: LLMBackend | None = None):
        super().__init__(name="data_lookup_sec", backend=backend or NoopLLMBackend())

    def get_prompt(self, *, previous_history_entries):
        return ""

    def process_prompt(self, *, result: str, previous_history_entries):
        return Run(stage_name="final_response")
