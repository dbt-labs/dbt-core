from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.snowflake import SnowflakeAdapter


def get_adapter(adapter_type):
    adapters = {
        'postgres': PostgresAdapter,
        'redshift': PostgresAdapter,
        'snowflake': SnowflakeAdapter,
    }

    return adapters[adapter_type]
