from __future__ import annotations

import logging
import time
from typing import Any, Mapping, Optional

from profit.agent.llm import BaseLLM, RetryConfig

logger = logging.getLogger(__name__)

try:
    # New SDK entrypoint
    from openai import OpenAI
except ImportError:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[misc]


class ChatGPTLLM(BaseLLM):
    """
    OpenAI LLM wrapper using the Responses API.

    - Uses client.responses.create(...)
    - Extracts text via response.output_text
    """

    def __init__(
        self,
        *,
        model: str = "gpt-5-nano",
        temperature: float = 0.0,
        max_tokens: int = 512,
        system_prompt: Optional[str] = None,
        retry_policy: Optional[RetryConfig] = None,
        model_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if OpenAI is None:
            raise RuntimeError("The openai package is required for ChatGPTLLM.")

        super().__init__(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            system_prompt=system_prompt,
            model_kwargs=model_kwargs,
        )
        self.retry_policy = retry_policy or RetryConfig()

        # Create a reusable client once (reads OPENAI_API_KEY from env by default).
        self._client = OpenAI()

        # Normalize kwargs so **self.model_kwargs is safe.
        self.model_kwargs = dict(model_kwargs or {})

        # Ensure a default system prompt string (Responses API supports `instructions`,
        # or you can pass it as a system role in `input`; this implementation uses input roles).
        self.system_prompt = system_prompt or ""

    def _send(self, prompt: str) -> str:
        attempt = 0

        # Split out parameters that belong to the Responses API vs common knobs.
        # temperature is supported for many models; keep it here if your models accept it.
        extra_kwargs = dict(self.model_kwargs)
        extra_kwargs.setdefault("temperature", self.temperature)

        while True:
            attempt += 1
            try:
                response = self._client.responses.create(
                    model=self.model,
                    input=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    max_output_tokens=self.max_tokens,
                    **extra_kwargs,
                )

                text = (response.output_text or "").strip()

                usage = getattr(response, "usage", None)
                total_tokens = getattr(usage, "total_tokens", 0) if usage else 0
                logger.debug("ChatGPTLLM received %d tokens", total_tokens)

                # If you ever see empty text with nonzero tokens, it can be because the
                # model hit max_output_tokens before emitting visible output.
                # In that case consider increasing max_tokens or checking response.status.
                return text

            except Exception as exc:
                if attempt >= self.retry_policy.max_attempts:
                    logger.error("ChatGPTLLM failed after %d attempts", attempt)
                    raise
                sleep_time = self.retry_policy.backoff_seconds * attempt
                logger.warning(
                    "ChatGPTLLM attempt %d failed (%s); retrying in %.1fs",
                    attempt,
                    exc,
                    sleep_time,
                )
                time.sleep(sleep_time)
