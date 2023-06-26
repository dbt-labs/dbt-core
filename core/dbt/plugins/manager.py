import importlib
import pkgutil
from typing import Dict

from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import DbtRuntimeError
from dbt.plugins.contracts import ExternalArtifacts
from dbt.plugins.manifest import ExternalNodes


def dbt_hook(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise DbtRuntimeError(f"{func.__name__}: {e}")

    setattr(inner, "is_dbt_hook", True)
    return inner


class dbtPlugin:
    def __init__(self):
        pass

    def get_external_nodes(self) -> ExternalNodes:
        """TODO"""
        raise NotImplementedError

    def get_external_artifacts(
        self, manifest: Manifest, project_name: str, adapter_type: str, quoting: Dict[str, str]
    ) -> ExternalArtifacts:
        """TODO"""
        raise NotImplementedError


class PluginManager:
    PLUGIN_PREFIX = "dbt_"

    def __init__(self):
        discovered_dbt_modules = {
            name: importlib.import_module(name)
            for _, name, _ in pkgutil.iter_modules()
            if name.startswith(self.PLUGIN_PREFIX)
        }

        plugins = {}
        for name, module in discovered_dbt_modules.items():
            if hasattr(module, "plugin"):
                plugin_cls = getattr(module, "plugin")
                assert issubclass(
                    plugin_cls, dbtPlugin
                ), f"'plugin' in {name} must be subclass of dbtPlugin"

                plugin = plugin_cls()
                plugins[name] = plugin

        valid_hook_names = set()
        # default hook implementations from dbtPlugin
        for hook_name in dir(dbtPlugin):
            if not hook_name.startswith("_"):
                valid_hook_names.add(hook_name)

        self.hooks = {}
        for plugin_cls in plugins.values():
            for hook_name in dir(plugin):
                hook = getattr(plugin, hook_name)
                if (
                    callable(hook)
                    and hasattr(hook, "is_dbt_hook")
                    and hook_name in valid_hook_names
                ):
                    if hook_name in self.hooks:
                        self.hooks[hook_name].append(hook)
                    else:
                        self.hooks[hook_name] = [hook]

    def get_external_artifacts(
        self, manifest: Manifest, project_name: str, adapter_type: str, quoting: Dict[str, str]
    ) -> ExternalArtifacts:
        external_artifacts = {}
        for hook_method in self.hooks.get("get_external_artifacts", []):
            plugin_external_artifact = hook_method(manifest, project_name, adapter_type, quoting)
            external_artifacts.update(plugin_external_artifact)
        return external_artifacts

    def get_external_nodes(self) -> ExternalNodes:
        external_nodes = ExternalNodes()
        for hook_method in self.hooks.get("get_external_nodes", []):
            plugin_external_nodes = hook_method()
            external_nodes.update(plugin_external_nodes)
        return external_nodes
