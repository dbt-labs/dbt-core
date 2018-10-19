from dbt.adapters.postgres.connections import PostgresConnectionManager
from dbt.adapters.postgres.connections import PostgresCredentials
from dbt.adapters.postgres.impl import PostgresAdapter
from dbt.adapters.factory import register_adapter_type

register_adapter_type(PostgresAdapter, PostgresCredentials)
