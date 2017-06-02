from dbt.logger import GLOBAL_LOGGER as logger

import platform
import pip

import dbt.exceptions

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftAdapter
from dbt.adapters.snowflake import SnowflakeAdapter


def get_adapter(profile):
    adapter_type = profile.get('type', None)

    adapters = {
        'postgres': PostgresAdapter,
        'redshift': RedshiftAdapter,
        'snowflake': SnowflakeAdapter
    }

    adapter = adapters.get(adapter_type, None)

    if adapter is None:
        raise RuntimeError(
            "Invalid adapter type {}!"
            .format(adapter_type))

    try:
        adapter.initialize()
    except ImportError as e:
        logger.debug(e)
        logger.info("Installing required libraries for {}".format(adapter_type))
        adapter.install_requires()

        # try again
        adapter.initialize()

    return adapter
