from __future__ import annotations


class AgentV2Error(RuntimeError):
    pass


class AgentV2ValidationError(AgentV2Error, ValueError):
    pass

