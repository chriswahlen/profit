from __future__ import annotations

import logging
import os
from typing import Any, Dict

from agentapi.components.snapshot_store import SnapshotStore
from agentapi.state_machine import StateMachine
from config import Config
from data_sources.data_source_manager import DataSourceManager
from llm.llm_backend import LLMBackend
from llm.stub_llm import StubLLM
from service.queue import JobQueue

from agents.financial_adviser.state_machine import build_financial_adviser_state_machine

logger = logging.getLogger(__name__)

JOB_TYPE_FINANCIAL_ADVISER = "financial_adviser"

def _resolve_openai_api_key(config: Config) -> str | None:
    """
    Resolve the OpenAI API key.

    Priority:
    1) Environment variable OPENAI_API_KEY
    2) Profit config file key OPENAI_API_KEY (or openai_api_key)
    """

    env_key = os.environ.get("OPENAI_API_KEY")
    if env_key:
        return env_key
    # Support both styles; configparser is case-insensitive but callers sometimes vary.
    key = config.get_key("OPENAI_API_KEY") or config.get_key("openai_api_key")
    return key.strip() if isinstance(key, str) and key.strip() else None


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
            default='{"action":"answer","plan":{"description":"Provide a safe educational response.","instructions":"Be concise; avoid personalized advice; if the question depends on EDGAR facts, request queries."},"goal":"Provide an initial helpful answer.","answer":"(stub) I can share general educational information, not personalized financial advice."}',
        )

    cfg = Config()
    api_key = _resolve_openai_api_key(cfg)

    try:
        from llm.chatgpt_llm import ChatGPTLLM
        return ChatGPTLLM(api_key=api_key)
    except Exception as exc:  # noqa: BLE001
        # Don't prevent the worker from starting if OpenAI deps/keys are missing.
        logger.warning("live backend unavailable, falling back to StubLLM: %s", exc)
        hint = "Install the OpenAI SDK in the venv (e.g., `pip install openai`) and/or set OPENAI_API_KEY."
        if api_key and not os.environ.get("OPENAI_API_KEY"):
            hint = "OpenAI key was found in Profit config, but OpenAI SDK may be missing. Install it (e.g., `pip install openai`)."
        return StubLLM(
            key_responses={},
            default=f'{{"action":"answer","answer":"(stub) Live LLM backend unavailable. {hint}"}}',
        )


def _build_machine(execution_id: str, payload: Dict[str, Any], store: SnapshotStore, *, live: bool) -> StateMachine:
    question = _extract_question(payload)
    backend = _resolve_backend(live=live)
    mgr = DataSourceManager(Config())
    machine, _store = build_financial_adviser_state_machine(
        execution_id=execution_id,
        question=question,
        snapshot_store=store,
        llm_backend=backend,
        edgar_store=mgr.edgar_store,
    )
    # Ensure we close our shared stores even when a job fails.
    machine._cleanup_callbacks = [mgr.entity_store.close, mgr.edgar_store.close]  # type: ignore[attr-defined]
    return machine


def register_jobs(queue: JobQueue, *, live: bool = False) -> None:
    queue.register_job_type(
        JOB_TYPE_FINANCIAL_ADVISER,
        lambda execution_id, payload, store: _build_machine(execution_id, payload, store, live=live),
    )
