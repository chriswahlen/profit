from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

from agentapi.components.snapshot_store import Snapshot, SnapshotStore
from agentapi.components.memory_snapshot_store import MemorySnapshotStore
from agentapi.plan import Fork, Plan
from agentapi.runners import AgentTransformRunner
from agentapi.state_machine import StateMachine
from llm.llm_backend import LLMBackend
from llm.stub_llm import StubLLM

logger = logging.getLogger(__name__)

PLAN_SCHEMA_VERSION = 1
STAGE_REGISTRY_VERSION = "financial_adviser:v1"
STAGE_QA = "financial_adviser.qa"


@dataclass(frozen=True)
class FinancialAdviserPayload:
    question: str

    def to_user_context(self) -> dict[str, Any]:
        return {"financial_adviser": {"question": self.question}}


class FinancialAdviserQARunner(AgentTransformRunner):
    def __init__(self, *, backend: LLMBackend, model: str | None = None) -> None:
        super().__init__(name=STAGE_QA, backend=backend, model=model)

    def get_prompt(self, *, previous_history_entries: list[Any], user_context: dict[str, Any]) -> str:
        payload = user_context.get("financial_adviser")
        question = payload.get("question") if isinstance(payload, dict) else None
        if not isinstance(question, str) or not question.strip():
            raise ValueError("user_context.financial_adviser.question is required")

        # Keep this stage intentionally simple for now: one user question -> one answer.
        # Future stages can add clarifying questions, risk profile, account constraints, etc.
        return "\n".join(
            [
                "You are a financial adviser assistant.",
                "Provide general educational information only (not personalized financial advice).",
                "Be concise and include a short safety disclaimer.",
                "",
                f"User question: {question.strip()}",
            ]
        )

    def process_prompt(
        self,
        *,
        result: str,
        previous_history_entries: list[Any],
        user_context: dict[str, Any],
    ) -> Plan:
        user_context.setdefault("financial_adviser", {})
        fa = user_context["financial_adviser"]
        if not isinstance(fa, dict):
            raise ValueError("user_context.financial_adviser must be a dict")
        fa["answer"] = result
        fa["status"] = "completed"
        return Fork(children=[])


def _seed_snapshot_if_missing(
    *,
    execution_id: str,
    store: SnapshotStore,
    payload: FinancialAdviserPayload,
) -> None:
    existing = store.load_snapshot(execution_id)
    if existing is not None:
        return

    snapshot = Snapshot(
        plan_schema_version=PLAN_SCHEMA_VERSION,
        stage_registry_version=STAGE_REGISTRY_VERSION,
        start_run_id=None,
        terminal_state=None,
        terminal_reason=None,
        run_id_allocator_state=1,
        history_cursor=0,
        user_context_json=json.dumps(payload.to_user_context(), sort_keys=True, separators=(",", ":"), ensure_ascii=False),
    )
    store.save_snapshot(execution_id, snapshot=snapshot)
    logger.info("seeded financial adviser snapshot execution_id=%s", execution_id)


def build_financial_adviser_state_machine(
    *,
    execution_id: str,
    question: str,
    snapshot_store: Optional[SnapshotStore] = None,
    llm_backend: Optional[LLMBackend] = None,
    model: str | None = None,
) -> tuple[StateMachine, SnapshotStore]:
    """
    Creates a minimal Financial Adviser state machine:
    - Single `financial_adviser.qa` stage
    - Start == Terminal
    - Uses `user_context.financial_adviser.question` as input
    - Writes `user_context.financial_adviser.answer` as output
    """

    store = snapshot_store or MemorySnapshotStore()
    backend = llm_backend or StubLLM(
        key_responses={},
        default="I can share general education, but I’m not a licensed adviser. Consider diversification and fees.",
    )

    _seed_snapshot_if_missing(
        execution_id=execution_id,
        store=store,
        payload=FinancialAdviserPayload(question=question),
    )

    machine = StateMachine(
        execution_id=execution_id,
        snapshot_store=store,
        plan_schema_version=PLAN_SCHEMA_VERSION,
        stage_registry_version=STAGE_REGISTRY_VERSION,
    )
    machine.register_stage(
        STAGE_QA,
        FinancialAdviserQARunner(backend=backend, model=model),
        is_start=True,
        is_terminal=True,
    )
    machine.finalize()
    return machine, store

