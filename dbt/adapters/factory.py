from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftAdapter
from dbt.adapters.snowflake import SnowflakeAdapter


def get_adapter(adapter_type):
    adapters = {
        'postgres': PostgresAdapter,
        'redshift': RedshiftAdapter,
        'snowflake': SnowflakeAdapter,
    }

    return adapters[adapter_type]
