from dbt.logger import GLOBAL_LOGGER as logger

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftAdapter
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.adapters.bigquery import BigQueryAdapter


adapters = {
    'postgres': PostgresAdapter,
    'redshift': RedshiftAdapter,
    'snowflake': SnowflakeAdapter,
    'bigquery': BigQueryAdapter
}

def list_adapters():
    adapter_list = {}
    for name, adapter in adapters.items():
        adapter_list[name] = adapter.is_installed()
    return adapter_list


def install_adapter(adapter_name):
    adapter = adapters.get(adapter_name)
    if adapter is None:
        # TODO
        raise RuntimeError(
            "Invalid adapter type {}!"
            .format(adapter_type))
    else:
        adapter.install_requires()


def get_adapter(profile):
    adapter_type = profile.get('type', None)
    adapter = adapters.get(adapter_type, None)

    if adapter is None:
        raise RuntimeError(
            "Invalid adapter type {}!"
            .format(adapter_type))

    try:
        adapter.initialize()
    except ImportError as e:
        logger.debug(e)
        logger.info("TODO") # TODO
        raise RuntimeError("not installed")

    return adapter
