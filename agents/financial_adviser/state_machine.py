from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

from agents.financial_adviser.db_query import DbQueryStage, STAGE_DB_QUERY
from agents.financial_adviser.final_answer import FinalAnswerStage, STAGE_FINAL
from agents.financial_adviser.initial_prompt import InitialPromptStage, STAGE_INITIAL_PROMPT
from agentapi.components.snapshot_store import Snapshot, SnapshotStore
from agentapi.components.memory_snapshot_store import MemorySnapshotStore
from agentapi.plan import Plan
from agentapi.state_machine import StateMachine
from config import Config
from data_sources.edgar.edgar_data_store import EdgarDataStore
from llm.llm_backend import LLMBackend
from llm.stub_llm import StubLLM

logger = logging.getLogger(__name__)

PLAN_SCHEMA_VERSION = 1
STAGE_REGISTRY_VERSION = "financial_adviser:v2"


@dataclass(frozen=True)
class FinancialAdviserPayload:
    question: str

    def to_user_context(self) -> dict[str, Any]:
        return {
            "financial_adviser": {
                "question": self.question,
                "plan": None,
                "round": 0,
                "goal_by_round": {},
                "db_results": [],
            }
        }


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
    edgar_store: EdgarDataStore | None = None,
) -> tuple[StateMachine, SnapshotStore]:
    """
    Creates a minimal Financial Adviser state machine:
    - `financial_adviser.initial_prompt` (LLM) decides which SQL to run or answers
    - `financial_adviser.db_query` executes the SQL against `edgar.sqlite`
    - `financial_adviser.final_answer` marks completion
    """

    store = snapshot_store or MemorySnapshotStore()
    backend = llm_backend or StubLLM(
        key_responses={},
        default='{"action":"answer","plan":{"description":"Provide general educational guidance.","instructions":"Answer concisely and note limitations; ask for database-backed follow-ups when needed."},"goal":"Provide a safe, concise response.","answer":"(stub) I can share general education, but I’m not a licensed adviser. Consider diversification and fees."}',
    )
    edgar = edgar_store or EdgarDataStore(Config())

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
        STAGE_INITIAL_PROMPT,
        InitialPromptStage(
            backend=backend,
            model=model,
            db_query_stage_name=STAGE_DB_QUERY,
            final_stage_name=STAGE_FINAL,
        ),
        is_start=True,
    )
    machine.register_stage(
        STAGE_DB_QUERY,
        DbQueryStage(
            edgar_store=edgar,
            next_stage_name=STAGE_INITIAL_PROMPT,
            final_stage_name=STAGE_FINAL,
        ),
    )
    machine.register_stage(
        STAGE_FINAL,
        FinalAnswerStage(),
        is_terminal=True,
    )
    machine.finalize()
    return machine, store
