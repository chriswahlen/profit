"""Custom exceptions for the agent v2 runtime."""

from __future__ import annotations


class AgentV2ValidationError(ValueError):
    """Raised when an LLM response fails schema or business validation."""


class AgentV2RuntimeError(RuntimeError):
    """Raised when the agent runtime encounters an unrecoverable error."""

