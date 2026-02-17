from __future__ import annotations

import logging
from typing import Any, Dict

from agentapi.components.snapshot_store import SnapshotStore
from agentapi.state_machine import StateMachine
from llm.llm_backend import LLMBackend
from llm.stub_llm import StubLLM
from service.queue import JobQueue

from agents.financial_adviser.state_machine import build_financial_adviser_state_machine

logger = logging.getLogger(__name__)

JOB_TYPE_FINANCIAL_ADVISER = "financial_adviser"


def _extract_question(payload: Dict[str, Any]) -> str:
    question = payload.get("question")
    if isinstance(question, str) and question.strip():
        return question.strip()
    prompt = payload.get("prompt")
    if isinstance(prompt, str) and prompt.strip():
        return prompt.strip()
    raise ValueError("payload must contain non-empty 'question' or 'prompt'")


def _resolve_backend(*, live: bool) -> LLMBackend:
    if not live:
        return StubLLM(
            key_responses={},
            default="(stub) I can share general educational information, not personalized financial advice.",
        )

    try:
        from llm.chatgpt_llm import ChatGPTLLM

        return ChatGPTLLM()
    except Exception as exc:  # noqa: BLE001
        # Don't prevent the worker from starting if OpenAI deps/keys are missing.
        logger.warning("live backend unavailable, falling back to StubLLM: %s", exc)
        return StubLLM(
            key_responses={},
            default="(stub) Live LLM backend unavailable; provide OPENAI_API_KEY and install openai.",
        )


def _build_machine(execution_id: str, payload: Dict[str, Any], store: SnapshotStore, *, live: bool) -> StateMachine:
    question = _extract_question(payload)
    backend = _resolve_backend(live=live)
    machine, _store = build_financial_adviser_state_machine(
        execution_id=execution_id,
        question=question,
        snapshot_store=store,
        llm_backend=backend,
    )
    return machine


def register_jobs(queue: JobQueue, *, live: bool = False) -> None:
    queue.register_job_type(
        JOB_TYPE_FINANCIAL_ADVISER,
        lambda execution_id, payload, store: _build_machine(execution_id, payload, store, live=live),
    )

