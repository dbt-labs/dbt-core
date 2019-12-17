import threading
from importlib import import_module
from typing import Type, Dict, Any

from dbt.exceptions import RuntimeException
from dbt.include.global_project import PACKAGES
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.connection import Credentials, AdapterRequiredConfig

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.plugin import AdapterPlugin


# TODO: we can't import these because they cause an import cycle.
# Profile has to call into load_plugin to get credentials, so adapter/relation
# don't work
BaseRelation = Any


Adapter = BaseAdapter


class AdpaterContainer:
    def __init__(self):
        self.lock = threading.Lock()
        self.adapters: Dict[str, Adapter] = {}
        self.adapter_types: Dict[str, Type[Adapter]] = {}

    def get_adapter_class_by_name(self, name: str) -> Type[Adapter]:
        with self.lock:
            if name in self.adapter_types:
                return self.adapter_types[name]

            names = ", ".join(self.adapter_types.keys())

        message = f"Invalid adapter type {name}! Must be one of {names}"
        raise RuntimeException(message)

    def get_relation_class_by_name(self, name: str) -> Type[BaseRelation]:
        adapter = self.get_adapter_class_by_name(name)
        return adapter.Relation

    def load_plugin(self, name: str) -> Type[Credentials]:
        # this doesn't need a lock: in the worst case we'll overwrite PACKAGES
        # and adapter_type entries with the same value, as they're all
        # singletons
        try:
            mod = import_module('.' + name, 'dbt.adapters')
        except ImportError as e:
            logger.info("Error importing adapter: {}".format(e))
            raise RuntimeException(
                "Could not find adapter type {}!".format(name)
            )
        if not hasattr(mod, 'Plugin'):
            raise RuntimeException(
                f'Could not find plugin in {name} plugin module'
            )
        plugin: AdapterPlugin = mod.Plugin  # type: ignore
        plugin_type = plugin.adapter.type()

        if plugin_type != name:
            raise RuntimeException(
                f'Expected to find adapter with type named {name}, got '
                f'adapter with type {plugin_type}'
            )

        with self.lock:
            # things do hold the lock to iterate over it so we need it to add
            self.adapter_types[name] = plugin.adapter

        PACKAGES[plugin.project_name] = plugin.include_path

        for dep in plugin.dependencies:
            self.load_plugin(dep)

        return plugin.credentials

    def register_adapter(self, config: AdapterRequiredConfig) -> None:
        adapter_name = config.credentials.type
        adapter_type = self.get_adapter_class_by_name(adapter_name)

        with self.lock:
            if adapter_name in self.adapters:
                # this shouldn't really happen...
                return

            adapter: Adapter = adapter_type(config)  # type: ignore
            self.adapters[adapter_name] = adapter

    def lookup_adapter(self, adapter_name: str) -> Adapter:
        return self.adapters[adapter_name]

    def reset_adapters(self):
        """Clear the adapters. This is useful for tests, which change configs.
        """
        with self.lock:
            for adapter in self.adapters.values():
                adapter.cleanup_connections()
            self.adapters.clear()

    def cleanup_connections(self):
        """Only clean up the adapter connections list without resetting the
        actual adapters.
        """
        with self.lock:
            for adapter in self.adapters.values():
                adapter.cleanup_connections()


FACTORY: AdpaterContainer = AdpaterContainer()


def register_adapter(config: AdapterRequiredConfig) -> None:
    FACTORY.register_adapter(config)


def get_adapter(config: AdapterRequiredConfig):
    return FACTORY.lookup_adapter(config.credentials.type)


def reset_adapters():
    """Clear the adapters. This is useful for tests, which change configs.
    """
    FACTORY.reset_adapters()


def cleanup_connections():
    """Only clean up the adapter connections list without resetting the actual
    adapters.
    """
    FACTORY.cleanup_connections()


def get_adapter_class_by_name(name: str) -> Type[BaseAdapter]:
    return FACTORY.get_adapter_class_by_name(name)


def get_relation_class_by_name(name: str) -> Type[BaseRelation]:
    return FACTORY.get_relation_class_by_name(name)


def load_plugin(name: str) -> Type[Credentials]:
    return FACTORY.load_plugin(name)
