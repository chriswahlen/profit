from __future__ import annotations

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.retrievers import EdgarRetrieverV2


class DataLookupSecRunner(ContextualAgentRunner):
    """Stage 3c: fetch SEC/EDGAR data."""

    def __init__(self):
        # TODO: Implement

    def get_prompt(self, *, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")

    def process_prompt(self, *, result: str, previous_history_entries):
        # TODO: Implement
        raise Exception("aislop")
