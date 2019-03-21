from dbt.logger import GLOBAL_LOGGER as logger

import dbt.exceptions
from importlib import import_module
from dbt.include.global_project import PACKAGES

import threading

ADAPTER_TYPES = {}

_ADAPTERS = {}
_ADAPTER_LOCK = threading.Lock()


def get_adapter_class_by_name(adapter_name):
    with _ADAPTER_LOCK:
        if adapter_name in ADAPTER_TYPES:
            return ADAPTER_TYPES[adapter_name]

    message = "Invalid adapter type {}! Must be one of {}"
    adapter_names = ", ".join(ADAPTER_TYPES.keys())
    formatted_message = message.format(adapter_name, adapter_names)
    raise dbt.exceptions.RuntimeException(formatted_message)


def get_relation_class_by_name(adapter_name):
    adapter = get_adapter_class_by_name(adapter_name)
    return adapter.Relation


def load_plugin(adapter_name):
    try:
        mod = import_module('.'+adapter_name, 'dbt.adapters')
    except ImportError:
        raise dbt.exceptions.RuntimeException(
            "Could not find adapter type {}!".format(adapter_name)
        )
    plugin = mod.Plugin

    if plugin.adapter.type() != adapter_name:
        raise dbt.exceptions.RuntimeException(
            'Expected to find adapter with type named {}, got adapter with '
            'type {}'
            .format(adapter_name, plugin.adapter.type())
        )

    with _ADAPTER_LOCK:
        ADAPTER_TYPES[adapter_name] = plugin.adapter

    PACKAGES[plugin.project_name] = plugin.include_path

    for dep in plugin.dependencies:
        load_plugin(dep)

    return plugin.credentials


def get_adapter(config):
    adapter_name = config.credentials.type
    if adapter_name in _ADAPTERS:
        return _ADAPTERS[adapter_name]

    with _ADAPTER_LOCK:
        if adapter_name not in ADAPTER_TYPES:
            raise dbt.exceptions.RuntimeException(
                "Could not find adapter type {}!".format(adapter_name)
            )

        adapter_type = ADAPTER_TYPES[adapter_name]

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
        for adapter in _ADAPTERS.values():
            adapter.cleanup_connections()
        _ADAPTERS.clear()
