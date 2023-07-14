import importlib
import pkgutil
from typing import Dict, List, Callable

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
    """
    EXPERIMENTAL: dbtPlugin is the base class for creating plugins.
    Its interface is **not** stable and will likely change between dbt-core versions.
    """

    def __init__(self, project_name: str):
        self.project_name = project_name
        try:
            self.initialize()
        except Exception as e:
            raise DbtRuntimeError(f"initialize: {e}")

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def initialize(self) -> None:
        """
        Initialize the plugin. This function may be overridden by subclasses that have
        additional initialization steps.
        """
        pass

    def get_nodes(self) -> PluginNodes:
        """
        Provide PluginNodes to dbt for injection into dbt's DAG.
        Currently the only node types that are accepted are model nodes.
        """
        raise NotImplementedError(f"get_nodes hook not implemented for {self.name}")

    def get_manifest_artifacts(self, manifest: Manifest) -> PluginArtifacts:
        """
        Given a manifest, provide PluginArtifacts derived for writing by core.
        PluginArtifacts share the same lifecycle as the manifest.json file -- they
        will either be written or not depending on whether the manifest is written.
        """
        raise NotImplementedError(f"get_manifest_artifacts hook not implemented for {self.name}")


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
    def from_modules(cls, project_name: str) -> "PluginManager":
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
                    plugin = plugin_cls(project_name=project_name)
                    plugins.append(plugin)
        return cls(plugins=plugins)

    def get_manifest_artifacts(self, manifest: Manifest) -> PluginArtifacts:
        all_plugin_artifacts = {}
        for hook_method in self.hooks.get("get_manifest_artifacts", []):
            plugin_artifacts = hook_method(manifest)
            all_plugin_artifacts.update(plugin_artifacts)
        return all_plugin_artifacts

    def get_nodes(self) -> PluginNodes:
        all_plugin_nodes = PluginNodes()
        for hook_method in self.hooks.get("get_nodes", []):
            plugin_nodes = hook_method()
            all_plugin_nodes.update(plugin_nodes)
        return all_plugin_nodes
