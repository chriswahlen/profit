from __future__ import annotations

import json
import os
import logging
from datetime import date
from typing import Protocol

from profit.agent.types import Answer, Question, RetrievalPlan, RetrievedData
from profit.agent.prompt import build_messages

logger = logging.getLogger(__name__)


class ChatLLM(Protocol):
    def generate(self, *, question: Question, plan: RetrievalPlan, data: RetrievedData) -> Answer:
        ...


class StubLLM:
    """
    Deterministic stand-in for local development and tests.
    """

    def __init__(self, model: str | None = None) -> None:
        self.model = model or os.environ.get("PROFIT_AGENT_MODEL", "stub")

    def generate(self, *, question: Question, plan: RetrievalPlan, data: RetrievedData) -> Answer:
        summary = f"[stub model={self.model}] source={plan.source}"
        parts = []
        if plan.instruments:
            parts.append(f"instruments={','.join(plan.instruments)}")
        if plan.regions:
            parts.append(f"regions={','.join(plan.regions)}")
        if plan.filings:
            parts.append(f"filings={','.join(plan.filings)}")
        if plan.start or plan.end:
            parts.append(f"window={plan.start or 'None'}..{plan.end or 'None'}")
        if data.payload is not None:
            parts.append(f"payload_keys={list(data.payload) if hasattr(data.payload, 'keys') else 'value'}")
        text = "; ".join([summary, *parts]) if parts else summary
        # Log the would-be prompt and payload for visibility even in stub mode.
        plan_opts = _plan_prompt_options_stub(question, plan, data)
        logger.info("llm.plan_opts model=%s opts=%s", self.model, plan_opts)
        messages = build_messages(question=question, plan=plan, data=data, today=date.today(), plan_opts=plan_opts)
        logger.info("llm.prompt model=%s messages=%s", self.model, _summarize_messages(messages))
        logger.info("llm.data model=%s payload=%s", self.model, _serialize(data.payload))
        return Answer(text=text, supporting=data)


class ChatGPTLLM:
    """
    Thin wrapper around OpenAI Chat Completions API.
    """

    def __init__(
        self,
        *,
        model: str = "gpt-4.1-mini",
        api_key: str | None = None,
        today: date | None = None,
    ) -> None:
        self.model = model
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.today = today
        if self.api_key is None:
            raise RuntimeError("OPENAI_API_KEY not set; cannot use ChatGPTLLM")

    def generate(self, *, question: Question, plan: RetrievalPlan, data: RetrievedData) -> Answer:
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - depends on optional dep
            raise RuntimeError("openai package not installed; use StubLLM or install openai") from exc

        client = OpenAI(api_key=self.api_key)
        plan_opts = _plan_prompt_options(client, self.model, question, plan, data)
        logger.info("llm.plan_opts model=%s opts=%s", self.model, plan_opts)
        messages = build_messages(question=question, plan=plan, data=data, today=self.today, plan_opts=plan_opts)
        logger.info("llm.prompt model=%s messages=%s", self.model, _summarize_messages(messages))
        logger.info("llm.data model=%s payload=%s", self.model, _serialize(data.payload))
        resp = client.chat.completions.create(model=self.model, messages=messages, temperature=0.2, max_tokens=400)
        text = resp.choices[0].message.content
        return Answer(text=text or "", supporting=data)


def _summarize_messages(messages: list[dict], max_len: int = 4000) -> str:
    try:
        raw = json.dumps(messages, default=str)
    except TypeError:
        raw = str(messages)
    if len(raw) > max_len:
        return f"[len={len(raw)} truncated->{max_len}] " + raw[: max_len - 3] + "..."
    return raw


def _serialize(obj, max_len: int = 4000) -> str:
    try:
        raw = json.dumps(obj, default=str)
    except TypeError:
        raw = str(obj)
    if len(raw) > max_len:
        return f"[len={len(raw)} truncated->{max_len}] " + raw[: max_len - 3] + "..."
    return raw


def _plan_prompt_options(client, model: str, question: Question, plan: RetrievalPlan, data: RetrievedData) -> dict:
    """
    Ask the LLM to propose formatting options (max_points). Decimals are fixed in code.
    If it fails, fall back to defaults.
    """
    default = {"max_points": 365}
    try:
        summary = {
            "source": plan.source,
            "window": {"start": data.start.isoformat() if data.start else None, "end": data.end.isoformat() if data.end else None},
            "instruments": plan.instruments,
            "regions": plan.regions,
            "filings": plan.filings,
            "payload_size": len(json.dumps(data.payload, default=str)) if data.payload is not None else 0,
        }
        prompt = (
            "Choose a max_points integer for price series to keep the prompt concise. "
            "Return JSON like {\"max_points\": 40}. "
            "Prefer fewer points for long windows; keep between 10 and 365. "
            f"Context: {summary}"
        )
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": "You are a terse planner."}, {"role": "user", "content": prompt}],
            max_tokens=30,
            temperature=0.0,
        )
        content = resp.choices[0].message.content or ""
        plan_opts = json.loads(content)
        if not isinstance(plan_opts, dict):
            return default
        max_points = int(plan_opts.get("max_points", default["max_points"]))
        return {"max_points": max(10, min(max_points, 365))}
    except Exception:
        return default


def _plan_prompt_options_stub(question: Question, plan: RetrievalPlan, data: RetrievedData) -> dict:
    """
    Deterministic planner for stub mode; uses window length to scale max_points.
    """
    default = {"max_points": 30}
    try:
        if data.start and data.end:
            days = (data.end - data.start).days + 1
            max_points = min(365, max(10, days // 2))
        else:
            max_points = default["max_points"]
        return {"max_points": max_points}
    except Exception:
        return default
