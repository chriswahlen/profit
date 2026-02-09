from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List
import sys
from pathlib import Path

# Make vendored agentapi importable when running from the repo root.
_ROOT = Path(__file__).resolve().parents[2]
_AGENTAPI_PATH = _ROOT / "libs" / "agentapi"
if str(_AGENTAPI_PATH) not in sys.path:
    sys.path.insert(0, str(_AGENTAPI_PATH))

from llm import ChatGPTLLM, StubLLM, ThrottledLLM

__all__ = ["ChatGPTLLM", "StubLLM", "ThrottledLLM", "Question"]


@dataclass
class Question:
    text: str
    hints: List[str] = field(default_factory=list)

    def __init__(self, text: str, hints: Iterable[str] | None = None) -> None:
        self.text = text
        self.hints = list(hints or [])
