from __future__ import annotations

import logging
from typing import Iterable, Optional

from agentapi.state_machine import StateMachine
from agentapi.plan import Run

from profit.agent_v2.models import AgentV2RunnerConfig, Answer
from profit.agent_v2.runners import (
    InitialPromptRunner,
    QueryPriorInsightsRunner,
    CompileDataRunner,
    DataLookupMarketRunner,
    DataLookupRealEstateRunner,
    DataLookupSecRunner,
    FinalResponseRunner,
)

logger = logging.getLogger(__name__)


class AgentV2Runner:
    """AgentV2 powered by agentapi StateMachine + AgentTransformRunner stages."""

    def __init__(self, llm_backend, *, config: AgentV2RunnerConfig):
        self.config = config
        self.llm_backend = llm_backend
        self._answer_sink: dict = {}

    def _build_machine(self, question: str, hints: list[str], extra_instructions: str | None) -> StateMachine:
        machine = StateMachine(execution_id="agent_v2")

        machine.register_stage(
            "initial_prompt",
            InitialPromptRunner(
                backend=self.llm_backend,
                planner_path=self.config.planner_path,
                question=question,
                hints=hints,
                extra_instructions=extra_instructions,
            ),
            is_start=True,
        )
        machine.register_stage("query_prior_insights", QueryPriorInsightsRunner())
        machine.register_stage(
            "compile_data",
            CompileDataRunner(backend=self.llm_backend, compiler_path=self.config.compiler_path, question=question),
        )
        machine.register_stage("data_lookup_market", DataLookupMarketRunner())
        machine.register_stage("data_lookup_real_estate", DataLookupRealEstateRunner())
        machine.register_stage("data_lookup_sec", DataLookupSecRunner())
        machine.register_stage(
            "final_response",
            FinalResponseRunner(
                backend=self.llm_backend,
                prompt_path=self.config.final_prompt_path,
                question=question,
                answer_sink=self._answer_sink,
            ),
            is_terminal=True,
        )
        machine.finalize()
        return machine

    def run(self, *, question: str, hints: Optional[Iterable[str]] = None, extra_instructions: str | None = None) -> Answer:
        machine = self._build_machine(question, list(hints or []), extra_instructions)
        # Execute synchronously until terminal
        while True:
            ready = machine.poll_ready()
            for run in ready.runs:
                machine.execute_run(run.run_id or "")
            if ready.is_done:
                break
        answer: Answer = self._answer_sink.get("answer", Answer(text=""))
        return answer
