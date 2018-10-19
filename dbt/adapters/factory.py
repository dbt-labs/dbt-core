from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts import register_conection_contract

import dbt.exceptions
from importlib import import_module

import threading


ADAPTER_TYPES = {}

_ADAPTERS = {}
_ADAPTER_LOCK = threading.Lock()


def register_adapter_type(adapter_cls, contract_cls):
    """Given an adapter type and a contract type, register that adapter type.
    """
    adapter_type = adapter_cls.type()
    with _ADAPTER_LOCK:
        existing = ADAPTER_TYPES.get(adapter_type)
        if existing:
            msg = ('Got a duplicate adapter with type name {}, original {}, '
                   'new {}'.format(adapter_type, existing, adapter_cls))
            raise dbt.exceptions.RuntimeException(msg)
        ADAPTER_TYPES[adapter_type] = adapter_cls
        register_conection_contract(adapter_type, contract_cls)


def get_adapter_class_by_name(adapter_name):
    adapter_cls = ADAPTER_TYPES.get(adapter_name)
    # try to import it
    if adapter_cls is None:
        message = "Invalid adapter type {}! Must be one of {}"
        adapter_names = ", ".join(ADAPTER_TYPES.keys())
        formatted_message = message.format(adapter_name, adapter_names)
        raise dbt.exceptions.RuntimeException(formatted_message)

    else:
        return adapter_cls


def get_adapter(config):
    adapter_name = config.credentials.type
    if adapter_name in _ADAPTERS:
        return _ADAPTERS[adapter_name]

    with _ADAPTER_LOCK:
        adapter_type = get_adapter_class_by_name(adapter_name)
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
