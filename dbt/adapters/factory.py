import platform

import dbt.exceptions

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftAdapter

if platform.system() != 'Windows':
    from dbt.adapters.snowflake import SnowflakeAdapter
else:
    SnowflakeAdapter = None


def get_adapter(profile):
    adapter_type = profile.get('type', None)

    if platform.system() == 'Windows' and \
       adapter_type == 'snowflake':
        raise dbt.exceptions.NotImplementedException(
            "ERROR: 'snowflake' is not supported on Windows.")

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
