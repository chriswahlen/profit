from __future__ import annotations

import logging
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    text: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class RetryConfig:
    max_attempts: int = 3
    backoff_seconds: float = 1.0


class BaseLLM(ABC):
    def __init__(
        self,
        *,
        model: str,
        temperature: float = 0.0,
        max_tokens: int = 1024,
        system_prompt: str | None = None,
        model_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.system_prompt = system_prompt or "You are a helpful analytics agent."
        self.model_kwargs = model_kwargs or {}
        self._payload_counts: dict[str, int] = {}
        self._run_dir: Path | None = None

    def generate(
        self,
        *,
        question: Any,
        plan: Any | None = None,
        data: Any | None = None,
        prompt: str | None = None,
    ) -> LLMResponse:
        run_dir = self.begin_run() if self._run_dir is None else self._run_dir
        try:
            body = prompt or self._build_prompt(question=question, plan=plan, data=data)
            self._log_payload("prompt", body)
            text = self._send(body)
            self._log_payload("response", text)
            return LLMResponse(text=text, metadata={"model": self.model})
        finally:
            logger.info("LLM artifacts stored in %s", run_dir)

    def _build_prompt(self, *, question: Any, plan: Any | None, data: Any | None) -> str:
        parts: list[str] = []
        if question:
            parts.append(f"Query:\n{getattr(question, 'text', str(question))}")
        if plan:
            parts.append(f"Plan:\n{plan}")
        if data:
            parts.append(f"Data:\n{data}")
        return "\n\n".join(parts) if parts else ""

    PROMPT_SNIPPET_THRESHOLD = 4 * 1024

    def _log_payload(self, label: str, text: str) -> None:
        size = len(text.encode("utf-8"))
        snippet = self._make_snippet(text)
        run_dir = self._ensure_run_dir()
        base = "request" if label == "prompt" else label
        idx = self._payload_counts.get(base, 0) + 1
        self._payload_counts[base] = idx
        out_path = run_dir / f"{base}{idx}.txt"
        out_path.write_text(text, encoding="utf-8")
        logger.info(
            "%s payload (%d bytes) written to %s; snippet=%s",
            label,
            size,
            out_path,
            snippet,
        )

    @staticmethod
    def _make_snippet(text: str, chunk: int = 200) -> str:
        if len(text) <= chunk * 2:
            return text
        return f"{text[:chunk]}...{text[-chunk:]}"

    @abstractmethod
    def _send(self, prompt: str) -> str:
        ...

    def begin_run(self) -> Path:
        self._payload_counts = {}
        self._run_dir = self._create_run_dir()
        return self._run_dir

    def _create_run_dir(self) -> Path:
        base = Path(tempfile.gettempdir())
        dir_name = f"profit_agent_{int(time.time() * 1000)}"
        path = base / dir_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _ensure_run_dir(self) -> Path:
        if self._run_dir is None:
            self.begin_run()
        return self._run_dir

    def finalize_run(self) -> None:
        if self._run_dir:
            logger.info("LLM artifacts stored in %s", self._run_dir)


class StubLLM(BaseLLM):
    def __init__(
        self,
        *,
        model: str = "stub",
        response_map: Mapping[str, str] | None = None,
        default_response: str = "Stub response (no keyword match).",
    ) -> None:
        super().__init__(model=model)
        self.response_map = dict(response_map or {})
        self.default_response = default_response

    def set_response(self, keyword: str, response: str) -> None:
        self.response_map[keyword] = response

    def _send(self, prompt: str) -> str:
        lowered = prompt.lower()
        for keyword, response in self.response_map.items():
            if keyword.lower() in lowered:
                return response
        return self.default_response

