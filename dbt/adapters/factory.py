from dbt.logger import GLOBAL_LOGGER as logger

import dbt.exceptions
from importlib import import_module

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


def load_adapter(adapter_name):
    """Load an adapter package with the class of adapter_name, put it in the
    ADAPTER_TYPES dict, and return its associated Credentials
    """
    try:
        mod = import_module('.'+adapter_name, 'dbt.adapters')
    except ImportError:
        raise dbt.exceptions.RuntimeException(
            "Could not find adapter type {}!".format(adapter_name)
        )
    if mod.Adapter.type() != adapter_name:
        raise dbt.exceptions.RuntimeException(
            'Expected to find adapter with type named {}, got adapter with '
            'type {}'
            .format(adapter_name, mod.Adapter.type())
        )

    ADAPTER_TYPES[adapter_name] = mod.Adapter
    return mod.Credentials


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
        _ADAPTERS.clear()
        ADAPTER_TYPES.clear()
