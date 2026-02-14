from __future__ import annotations

# A DataSource implementation for Sqlite sources.
class SqliteDataSource(DataSource):
    def __init__(self, name: str, summary: str, config: Config):
      raise NotImplementedError("TODO")

    # This is a brief description, enough to give an Agent enough information to come up with the
    # type of query it could perform (but not necessarily the specifics).
    def describe_brief(self) -> str:
        return f"- {self.name}: {self.summary}"

    # Returns a prompt describing how to query this data source - tables, indices, the whole bit.
    def describe_detailed(self, *, indent: str = '  ') -> str:
        raise NotImplementedError("not implemented")