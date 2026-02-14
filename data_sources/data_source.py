from __future__ import annotations

class DataSourceUpdateResults:
    # TODO: a data class with the result of any update ops
    pass

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
    def describe_detailed(self, *, indent: str = '  ') -> str:
        raise NotImplementedError("not implemented")
    
    # Ensures that the given `entity_ids` are up-to-date for the given entity IDs.
    def ensure_up_to_date(self, entity_ids: list[str]) -> DataSourceUpdateResults:
        raise NotImplementedError("not implemented")
    
