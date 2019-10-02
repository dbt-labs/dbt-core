import threading
from importlib import import_module
from typing import Type, Dict, TypeVar

from dbt.exceptions import RuntimeException
from dbt.include.global_project import PACKAGES
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.connection import Credentials


# TODO: we can't import these because they cause an import cycle.
# currently RuntimeConfig needs to figure out default quoting for its adapter.
# We should push that elsewhere when we fixup project/profile stuff
# Instead here are some import loop avoiding-hacks right now. And Profile has
# to call into load_plugin to get credentials, so adapter/relation don't work
RuntimeConfig = TypeVar('RuntimeConfig')
BaseAdapter = TypeVar('BaseAdapter')
BaseRelation = TypeVar('BaseRelation')

ADAPTER_TYPES: Dict[str, Type[BaseAdapter]] = {}

_ADAPTERS: Dict[str, BaseAdapter] = {}
_ADAPTER_LOCK = threading.Lock()


def get_adapter_class_by_name(adapter_name: str) -> Type[BaseAdapter]:
    with _ADAPTER_LOCK:
        if adapter_name in ADAPTER_TYPES:
            return ADAPTER_TYPES[adapter_name]

        adapter_names = ", ".join(ADAPTER_TYPES.keys())

    message = "Invalid adapter type {}! Must be one of {}"
    formatted_message = message.format(adapter_name, adapter_names)
    raise RuntimeException(formatted_message)


def get_relation_class_by_name(adapter_name: str) -> Type[BaseRelation]:
    adapter = get_adapter_class_by_name(adapter_name)
    return adapter.Relation


def load_plugin(adapter_name: str) -> Credentials:
    # this doesn't need a lock: in the worst case we'll overwrite PACKAGES and
    # _ADAPTER_TYPE entries with the same value, as they're all singletons
    try:
        mod = import_module('.' + adapter_name, 'dbt.adapters')
    except ImportError as e:
        logger.info("Error importing adapter: {}".format(e))
        raise RuntimeException(
            "Could not find adapter type {}!".format(adapter_name)
        )
    plugin = mod.Plugin

    if plugin.adapter.type() != adapter_name:
        raise RuntimeException(
            'Expected to find adapter with type named {}, got adapter with '
            'type {}'
            .format(adapter_name, plugin.adapter.type())
        )

    with _ADAPTER_LOCK:
        # things do hold the lock to iterate over it so we need ot to add stuff
        ADAPTER_TYPES[adapter_name] = plugin.adapter

    PACKAGES[plugin.project_name] = plugin.include_path

    for dep in plugin.dependencies:
        load_plugin(dep)

    return plugin.credentials


def get_adapter(config: RuntimeConfig) -> BaseAdapter:
    adapter_name = config.credentials.type

    # Atomically check to see if we already have an adapter
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
        for adapter in _ADAPTERS.values():
            adapter.cleanup_connections()
        _ADAPTERS.clear()


def cleanup_connections():
    """Only clean up the adapter connections list without resetting the actual
    adapters.
    """
    with _ADAPTER_LOCK:
        for adapter in _ADAPTERS.values():
            adapter.cleanup_connections()
