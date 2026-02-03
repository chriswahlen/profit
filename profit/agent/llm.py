from __future__ import annotations

import logging
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Mapping

try:
    import openai
except ImportError:  # pragma: no cover - optional dependency
    openai = None

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

    def generate(
        self,
        *,
        question: Any,
        plan: Any | None = None,
        data: Any | None = None,
        prompt: str | None = None,
    ) -> LLMResponse:
        body = prompt or self._build_prompt(question=question, plan=plan, data=data)
        self._log_payload("prompt", body)
        text = self._send(body)
        self._log_payload("response", text)
        return LLMResponse(text=text, metadata={"model": self.model})

    def _build_prompt(self, *, question: Any, plan: Any | None, data: Any | None) -> str:
        parts: list[str] = []
        if question:
            parts.append(f"Question:\n{getattr(question, 'text', str(question))}")
        if plan:
            parts.append(f"Plan:\n{plan}")
        if data:
            parts.append(f"Data:\n{data}")
        return "\n\n".join(parts) if parts else ""

    PROMPT_SNIPPET_THRESHOLD = 4 * 1024

    def _log_payload(self, label: str, text: str) -> None:
        size = len(text.encode("utf-8"))
        if size <= self.PROMPT_SNIPPET_THRESHOLD:
            logger.info("%s payload (%d bytes): %s", label, size, text)
            return

        snippet = self._make_snippet(text)
        tmp = tempfile.NamedTemporaryFile(
            delete=False,
            prefix=f"agent_{label}_",
            suffix=".txt",
            mode="w",
            encoding="utf-8",
        )
        tmp.write(text)
        tmp.flush()
        tmp_path = tmp.name
        tmp.close()
        logger.info(
            "%s payload (%d bytes) written to %s; snippet=%s",
            label,
            size,
            tmp_path,
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


class ChatGPTLLM(BaseLLM):
    def __init__(
        self,
        *,
        model: str = "gpt-5-nano",
        temperature: float = 0.0,
        max_tokens: int = 512,
        system_prompt: str | None = None,
        retry_policy: RetryConfig | None = None,
        model_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        if openai is None:
            raise RuntimeError("The openai package is required for ChatGPTLLM.")
        super().__init__(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            system_prompt=system_prompt,
            model_kwargs=model_kwargs,
        )
        self.retry_policy = retry_policy or RetryConfig()

    def _send(self, prompt: str) -> str:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = openai.ChatCompletion.create(
                    model=self.model,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    **self.model_kwargs,
                )
                text = response.choices[0].message.content.strip()
                logger.debug("ChatGPTLLM received %d tokens", response.usage.total_tokens if getattr(response, "usage", None) else 0)
                return text
            except Exception as exc:
                if attempt >= self.retry_policy.max_attempts:
                    logger.error("ChatGPTLLM failed after %d attempts", attempt)
                    raise
                sleep_time = self.retry_policy.backoff_seconds * attempt
                logger.warning("ChatGPTLLM attempt %d failed (%s); retrying in %.1fs", attempt, exc, sleep_time)
                time.sleep(sleep_time)
