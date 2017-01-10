from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.snowflake import SnowflakeAdapter


def get_adapter(target):
    adapters = {
        'postgres': PostgresAdapter,
        'redshift': PostgresAdapter,
        'snowflake': SnowflakeAdapter,
    }

    return adapters[target.target_type]
