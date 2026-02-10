from __future__ import annotations

import logging
from typing import Iterable, Optional
from pathlib import Path

from agentapi.state_machine import StateMachine
from agentapi.plan import Run
from agentapi.components.sqlite_snapshot_store import SqliteSnapshotStore

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

    def _build_machine(self, question: str, hints: list[str], extra_instructions: str | None, *, reset_state: bool) -> StateMachine:
        snapshot_path = self.config.snapshot_path or Path(f"/tmp/agent_v2_state_{self.config.execution_id}.sqlite")
        if reset_state and snapshot_path.exists():
            snapshot_path.unlink()
        snapshot_store = SqliteSnapshotStore(db_path=snapshot_path)
        snapshot_store.open()
        self._snapshot_store = snapshot_store
        machine = StateMachine(execution_id=self.config.execution_id, snapshot_store=snapshot_store)
        self._machine = machine

        machine.register_stage(
            "initial_prompt",
            InitialPromptRunner(
                backend=self.llm_backend,
                question=question,
                hints=hints,
                extra_instructions=extra_instructions,
            ),
            is_start=True,
        )
        machine.register_stage("query_prior_insights", QueryPriorInsightsRunner(backend=self.llm_backend))
        machine.register_stage(
            "compile_data",
            CompileDataRunner(backend=self.llm_backend, compiler_path=self.config.compiler_path, question=question),
        )
        machine.register_stage("data_lookup_market", DataLookupMarketRunner(backend=self.llm_backend))
        machine.register_stage("data_lookup_real_estate", DataLookupRealEstateRunner(backend=self.llm_backend))
        machine.register_stage("data_lookup_sec", DataLookupSecRunner(backend=self.llm_backend))
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

    def run(
        self,
        *,
        question: str,
        hints: Optional[Iterable[str]] = None,
        extra_instructions: str | None = None,
        reset_retry: bool = False,
        reset_retry_cached: bool = False,
        retry_run_id: str | None = None,
        reset_state: bool = False,
    ) -> Answer:
        machine = self._build_machine(question, list(hints or []), extra_instructions, reset_state=reset_state)
        try:
            forced_continue = False
            initial_ready = machine.poll_ready()

            if reset_retry or reset_retry_cached:
                run_id = retry_run_id
                if run_id is None:
                    try:
                        failed_ids = [rid for rid, node in machine._runs.items() if getattr(node, "status", "") == "failed"]  # type: ignore[attr-defined]
                        run_id = failed_ids[0] if failed_ids else None
                    except Exception:
                        run_id = None
                if run_id is None:
                    try:
                        run_id = machine._root_run_id()  # type: ignore[attr-defined]
                    except Exception:
                        logger.warning("could not resolve root run id for retry")
                if run_id:
                    machine.reset_retry_count(run_id)
                    logger.info("reset retry count for run_id=%s cached=%s", run_id, reset_retry_cached)
                    forced_continue = True

            ready = initial_ready if not forced_continue else None
            while True:
                if ready is None:
                    ready = machine.poll_ready()
                if ready.is_done and not forced_continue:
                    break
                forced_continue = False
                for run in ready.runs:
                    machine.execute_run(run.run_id or "")
                ready = None

            answer: Answer = self._answer_sink.get("answer", Answer(text=""))
            return answer
        finally:
            store = getattr(self, "_snapshot_store", None)
            if store is not None:
                try:
                    store.close()
                except Exception:
                    logger.warning("failed to close snapshot store", exc_info=True)
            if reset_state:
                try:
                    snapshot_path = self.config.snapshot_path or Path(f"/tmp/agent_v2_state_{self.config.execution_id}.sqlite")
                    if snapshot_path.exists():
                        snapshot_path.unlink()
                except Exception:
                    logger.warning("failed to delete snapshot path", exc_info=True)
