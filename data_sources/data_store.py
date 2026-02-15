
"""Base interfaces for data stores used by data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class DataSourceUpdateResults:
    """Summary of an update/ingest operation."""

    updated: int = 0
    skipped: int = 0
    failed: int = 0
    detail: str | None = None


class DataSourceStore(ABC):
    """The actual datastore implementation for a data source."""

    @abstractmethod
    def describe_brief(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def describe_detailed(self, *, indent: str = "  ") -> str:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
