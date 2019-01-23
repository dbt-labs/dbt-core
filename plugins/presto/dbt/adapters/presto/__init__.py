from dbt.adapters.presto.connections import PrestoConnectionManager
from dbt.adapters.presto.connections import PrestoCredentials
from dbt.adapters.presto.impl import PrestoAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import presto


Plugin = AdapterPlugin(
    adapter=PrestoAdapter,
    credentials=PrestoCredentials,
    include_path=presto.PACKAGE_PATH)
