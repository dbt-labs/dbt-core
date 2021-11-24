import threading
from pathlib import Path
from importlib import import_module
from typing import Type, Dict, Any, List, Optional, Set

from dbt.exceptions import RuntimeException, InternalException
from dbt.include.global_project import (
    PACKAGE_PATH as GLOBAL_PROJECT_PATH,
    PROJECT_NAME as GLOBAL_PROJECT_NAME,
)
from dbt.events.functions import fire_event
from dbt.events.types import AdapterImportError, PluginLoadError
from dbt.contracts.connection import Credentials, AdapterRequiredConfig
from dbt.adapters.protocol import (
    AdapterProtocol,
    AdapterConfig,
    RelationProtocol,
)
from dbt.adapters.base.plugin import AdapterPlugin


Adapter = AdapterProtocol


class AdapterContainer:
    def __init__(self):
        self.lock = threading.Lock()
        self.adapters: Dict[str, Adapter] = {}
        self.plugins: Dict[str, AdapterPlugin] = {}
        # map package names to their include paths
        self.packages: Dict[str, Path] = {
            GLOBAL_PROJECT_NAME: Path(GLOBAL_PROJECT_PATH),
        }

    def get_plugin_by_name(self, name: str) -> AdapterPlugin:
        with self.lock:
            if name in self.plugins:
                return self.plugins[name]
            names = ", ".join(self.plugins.keys())

        message = f"Invalid adapter type {name}! Must be one of {names}"
        raise RuntimeException(message)

    def get_adapter_class_by_name(self, name: str) -> Type[Adapter]:
        plugin = self.get_plugin_by_name(name)
        return plugin.adapter

    def get_relation_class_by_name(self, name: str) -> Type[RelationProtocol]:
        adapter = self.get_adapter_class_by_name(name)
        return adapter.Relation

    def get_config_class_by_name(
        self, name: str
    ) -> Type[AdapterConfig]:
        adapter = self.get_adapter_class_by_name(name)
        return adapter.AdapterSpecificConfigs

    def load_plugin(self, name: str) -> Type[Credentials]:
        # this doesn't need a lock: in the worst case we'll overwrite packages
        # and adapter_type entries with the same value, as they're all
        # singletons
        try:
            # mypy doesn't think modules have any attributes.
            mod: Any = import_module('.' + name, 'dbt.adapters')
        except ModuleNotFoundError as exc:
            # if we failed to import the target module in particular, inform
            # the user about it via a runtime error
            if exc.name == 'dbt.adapters.' + name:
                fire_event(AdapterImportError(exc=exc))
                raise RuntimeException(f'Could not find adapter type {name}!')
            # otherwise, the error had to have come from some underlying
            # library. Log the stack trace.

            fire_event(PluginLoadError())
            raise
        plugin: AdapterPlugin = mod.Plugin
        plugin_type = plugin.adapter.type()

        if plugin_type != name:
            raise RuntimeException(
                f'Expected to find adapter with type named {name}, got '
                f'adapter with type {plugin_type}'
            )

        with self.lock:
            # things do hold the lock to iterate over it so we need it to add
            self.plugins[name] = plugin

        self.packages[plugin.project_name] = Path(plugin.include_path)

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

    def get_adapter_plugins(self, name: Optional[str]) -> List[AdapterPlugin]:
        """Iterate over the known adapter plugins. If a name is provided,
        iterate in dependency order over the named plugin and its dependencies.
        """
        if name is None:
            return list(self.plugins.values())

        plugins: List[AdapterPlugin] = []
        seen: Set[str] = set()
        plugin_names: List[str] = [name]
        while plugin_names:
            plugin_name = plugin_names[0]
            plugin_names = plugin_names[1:]
            try:
                plugin = self.plugins[plugin_name]
            except KeyError:
                raise InternalException(
                    f'No plugin found for {plugin_name}'
                ) from None
            plugins.append(plugin)
            seen.add(plugin_name)
            if plugin.dependencies is None:
                continue
            for dep in plugin.dependencies:
                if dep not in seen:
                    plugin_names.append(dep)
        return plugins

    def get_adapter_package_names(self, name: Optional[str]) -> List[str]:
        package_names: List[str] = [
            p.project_name for p in self.get_adapter_plugins(name)
        ]
        package_names.append(GLOBAL_PROJECT_NAME)
        return package_names

    def get_include_paths(self, name: Optional[str]) -> List[Path]:
        paths = []
        for package_name in self.get_adapter_package_names(name):
            try:
                path = self.packages[package_name]
            except KeyError:
                raise InternalException(
                    f'No internal package listing found for {package_name}'
                )
            paths.append(path)
        return paths

    def get_adapter_type_names(self, name: Optional[str]) -> List[str]:
        return [p.adapter.type() for p in self.get_adapter_plugins(name)]


FACTORY: AdapterContainer = AdapterContainer()


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


def get_adapter_class_by_name(name: str) -> Type[AdapterProtocol]:
    return FACTORY.get_adapter_class_by_name(name)


def get_config_class_by_name(name: str) -> Type[AdapterConfig]:
    return FACTORY.get_config_class_by_name(name)


def get_relation_class_by_name(name: str) -> Type[RelationProtocol]:
    return FACTORY.get_relation_class_by_name(name)


def load_plugin(name: str) -> Type[Credentials]:
    return FACTORY.load_plugin(name)


def get_include_paths(name: Optional[str]) -> List[Path]:
    return FACTORY.get_include_paths(name)


def get_adapter_package_names(name: Optional[str]) -> List[str]:
    return FACTORY.get_adapter_package_names(name)


def get_adapter_type_names(name: Optional[str]) -> List[str]:
    return FACTORY.get_adapter_type_names(name)
