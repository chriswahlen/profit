# Configuration file 
# Loads from ~/.profit.conf by default.

class Config:
  def __init__(self):
    raise NotImplementedError("TODO")
  
  # Returns the path for storing the data files.
  def data_path(self) -> str:
    raise NotImplementedError("TODO")

  # Returns the value defined by the given key from the config.
  def get_key(self, key_name: str) -> str:
    raise NotImplementedError("TODO")
