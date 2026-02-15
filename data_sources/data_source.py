from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from config import Config
from data_sources.data_store import DataSourceUpdateResults


# Abstract base class describing a data source.
class DataSource(ABC):
    def __init__(self, name: str, summary: str, config: Config):
        self.name = name
        self.summary = summary
        self.config = config

    # This is a brief description, enough to give an Agent enough information to come up with the
    # type of query it could perform (but not necessarily the specifics).
    def describe_brief(self) -> str:
        return f"- {self.name}: {self.summary}"

    # Returns a prompt describing how to query this data source - tables, indices, the whole bit.
    @abstractmethod
    def describe_detailed(self, *, indent: str = '  ') -> str:
        raise NotImplementedError("not implemented")
    
    # Ensures that the given `entity_ids` are up-to-date for the given entity IDs.
    @abstractmethod
    def ensure_up_to_date(self, entity_ids: Iterable[str]) -> DataSourceUpdateResults:
        raise NotImplementedError("not implemented")
    
