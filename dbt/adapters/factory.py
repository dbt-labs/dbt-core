from dbt.logger import GLOBAL_LOGGER as logger

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftAdapter
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.adapters.bigquery import BigQueryAdapter

import dbt.exceptions


adapters = {
    'postgres': PostgresAdapter,
    'redshift': RedshiftAdapter,
    'snowflake': SnowflakeAdapter,
    'bigquery': BigQueryAdapter
}

ADAPTER_NOT_INSTALLED_ERROR = """
The required adapter for {adapter} is not installed.
To install it, please run the following commmand:

dbt adapter --install {adapter}

The following modules will be installed:
{module_list}
"""

def list_adapters():
    adapter_list = {}
    for name, adapter in adapters.items():
        adapter_list[name] = adapter.is_installed()
    return adapter_list


def get_adapter_by_name(adapter_name):
    adapter = adapters.get(adapter_name, None)

    if adapter is None:
        message = "Invalid adapter type {}! Must be one of {}"
        adapter_names = ", ".join(adapters.keys())
        raise RuntimeError(message.format(adapter_name, adapter_names))

    else:
        return adapter


def install_adapter(adapter_name):
    adapter = get_adapter_by_name(adapter_name)
    try:
        adapter.install_requires()
    except Exception as e:
        raise dbt.exceptions.RuntimeException(e)


def not_installed_error(adapter):
    raise dbt.exceptions.RuntimeException(ADAPTER_NOT_INSTALLED_ERROR.format(
        adapter=adapter.type(),
        module_list=", ".join(adapter.requires.values())))


def get_adapter(profile):
    adapter_type = profile.get('type', None)
    adapter = get_adapter_by_name(adapter_type)

    try:
        adapter.initialize()
    except ImportError as e:
        logger.debug(e)
        not_installed_error(adapter)

    return adapter
