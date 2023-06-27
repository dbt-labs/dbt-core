import importlib
import pkgutil
from typing import Dict, List, Callable

from dbt.config.project import Project
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import DbtRuntimeError
from dbt.plugins.contracts import PluginArtifacts
from dbt.plugins.manifest import PluginNodes


def dbt_hook(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise DbtRuntimeError(f"{func.__name__}: {e}")

    setattr(inner, "is_dbt_hook", True)
    return inner


class dbtPlugin:
    def __init__(self, name: str, project: Project):
        self.name = name
        self.project = project

    def get_nodes(self) -> PluginNodes:
        """TODO"""
        raise NotImplementedError

    def get_manifest_artifacts(self, manifest: Manifest) -> PluginArtifacts:
        """TODO"""
        raise NotImplementedError


class PluginManager:
    PLUGIN_MODULE_PREFIX = "dbt_"
    PLUGIN_ATTR_NAME = "plugins"

    def __init__(self, plugins: List[dbtPlugin]):
        self._plugins = plugins
        self._valid_hook_names = set()
        # default hook implementations from dbtPlugin
        for hook_name in dir(dbtPlugin):
            if not hook_name.startswith("_"):
                self._valid_hook_names.add(hook_name)

        self.hooks: Dict[str, List[Callable]] = {}
        for plugin in self._plugins:
            for hook_name in dir(plugin):
                hook = getattr(plugin, hook_name)
                if (
                    callable(hook)
                    and hasattr(hook, "is_dbt_hook")
                    and hook_name in self._valid_hook_names
                ):
                    if hook_name in self.hooks:
                        self.hooks[hook_name].append(hook)
                    else:
                        self.hooks[hook_name] = [hook]

    @classmethod
    def from_modules(cls, project: Project) -> "PluginManager":
        discovered_dbt_modules = {
            name: importlib.import_module(name)
            for _, name, _ in pkgutil.iter_modules()
            if name.startswith(cls.PLUGIN_MODULE_PREFIX)
        }

        plugins = []
        for name, module in discovered_dbt_modules.items():
            if hasattr(module, cls.PLUGIN_ATTR_NAME):
                available_plugins = getattr(module, cls.PLUGIN_ATTR_NAME, [])
                for plugin_cls in available_plugins:
                    assert issubclass(
                        plugin_cls, dbtPlugin
                    ), f"'plugin' in {name} must be subclass of dbtPlugin"

                    plugin = plugin_cls(name=name, project=project)
                    plugins.append(plugin)
        return cls(plugins=plugins)

    def get_manifest_artifacts(
        self, manifest: Manifest, project_name: str, adapter_type: str, quoting: Dict[str, str]
    ) -> PluginArtifacts:
        plugin_artifacts = {}
        for hook_method in self.hooks.get("get_manifest_artifacts", []):
            plugin_artifact = hook_method(manifest)
            plugin_artifacts.update(plugin_artifact)
        return plugin_artifacts

    def get_nodes(self) -> PluginNodes:
        plugin_nodes = PluginNodes()
        for hook_method in self.hooks.get("get_nodes", []):
            plugin_nodes = hook_method()
            plugin_nodes.update(plugin_nodes)
        return plugin_nodes
