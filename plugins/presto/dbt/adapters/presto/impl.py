from dbt.adapters.sql import SQLAdapter
from dbt.adapters.presto import PrestoConnectionManager


class PrestoAdapter(SQLAdapter):
    ConnectionManager = PrestoConnectionManager
