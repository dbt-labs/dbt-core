from dbt.logger import GLOBAL_LOGGER as logger

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftAdapter
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.adapters.bigquery import BigQueryAdapter

import dbt.exceptions

import threading


ADAPTER_TYPES = {
    'postgres': PostgresAdapter,
    'redshift': RedshiftAdapter,
    'snowflake': SnowflakeAdapter,
    'bigquery': BigQueryAdapter
}

_ADAPTERS = {}
_ADAPTER_LOCK = threading.Lock()


def get_adapter_class_by_name(adapter_name):
    adapter = ADAPTER_TYPES.get(adapter_name, None)

    if adapter is None:
        message = "Invalid adapter type {}! Must be one of {}"
        adapter_names = ", ".join(ADAPTER_TYPES.keys())
        formatted_message = message.format(adapter_name, adapter_names)
        raise dbt.exceptions.RuntimeException(formatted_message)

    else:
        return adapter


def get_adapter(config):
    adapter_name = config.credentials.type
    if adapter_name in _ADAPTERS:
        return _ADAPTERS[adapter_name]

    adapter_type = get_adapter_class_by_name(adapter_name)
    with _ADAPTER_LOCK:
        # check again, in case something was setting it before
        if adapter_name in _ADAPTERS:
            return _ADAPTERS[adapter_name]

        adapter = adapter_type(config)
        _ADAPTERS[adapter_name] = adapter
        return adapter


def reset_adapters():
    """Clear the adapters. This is useful for tests, which change configs.
    """
    with _ADAPTER_LOCK:
        _ADAPTERS.clear()


def get_relation_class_by_name(adapter_name):
    adapter = get_adapter_class_by_name(adapter_name)
    return adapter.Relation
