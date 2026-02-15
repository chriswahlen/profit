

# A helper class for dealing with "batch" data sources. What it does:
# Creates a table on the data source's database recording when the last time the "batch" was updated
# Provides a "maybe_update" method that checks the table's last update time, and if it's past a
# certain time, downloads a new bundle (provided by the data source somehow), and inserts new
# entries int the database using a provided DataSourceStore.
class BatchDataSourceHelper(BaseFetcher):
  # TODO