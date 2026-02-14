

class DataSourceManager:
  def __init__(self):
    raise NotImplementedError("TODO")

  # Adds a known data source to this manager.  
  def add(self, source: DataSource):
    raise NotImplementedError("TODO")

  # Returns the data source of the given key.
  def get(self, source_name: str) -> DataSource:
    raise NotImplementedError("TODO")
