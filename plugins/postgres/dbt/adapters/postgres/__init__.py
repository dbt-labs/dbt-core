# these are mostly just exports, #noqa them so flake8 will be happy
from dbt.adapters.postgres.connections import PostgresConnectionManager  # noqa
from dbt.adapters.postgres.connections import PostgresCredentials
from dbt.adapters.postgres.relation import PostgresColumn  # noqa
from dbt.adapters.postgres.relation import PostgresRelation  # noqa: F401
from dbt.adapters.postgres.impl import PostgresAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import postgres

Plugin = AdapterPlugin(
    adapter=PostgresAdapter,
    credentials=PostgresCredentials,
    include_path=postgres.PACKAGE_PATH)
