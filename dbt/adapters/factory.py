import platform

import dbt.exceptions

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftAdapter
from dbt.adapters.snowflake import SnowflakeAdapter


def get_adapter(profile):
    adapter_type = profile.get('type', None)

    adapters = {
        'postgres': PostgresAdapter,
        'redshift': RedshiftAdapter,
        'snowflake': SnowflakeAdapter,
    }

    adapter = adapters.get(adapter_type, None)

    if adapter is None:
        raise RuntimeError(
            "Invalid adapter type {}!"
            .format(adapter_type))

    return adapter
