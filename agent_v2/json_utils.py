from __future__ import annotations

import json
from typing import Any, Mapping

from agentapi.errors import CachedRetryable


def parse_json_object(result: str, *, stage: str) -> Mapping[str, Any]:
    """
    Parse a JSON object response from an LLM.

    On invalid JSON or non-object payloads, raises CachedRetryable so the engine can retry
    while caching the LLM response for deterministic reruns.
    """

    try:
        parsed = json.loads(result)
    except json.JSONDecodeError as exc:  # pragma: no cover (error path exercised in tests elsewhere)
        raise CachedRetryable(result=result, message=f"{stage}: invalid JSON ({exc})") from exc
    if not isinstance(parsed, dict):
        raise CachedRetryable(result=result, message=f"{stage}: expected JSON object at top-level")
    return parsed

