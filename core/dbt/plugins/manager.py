import importlib
import pkgutil
from typing import List, Dict

from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.contracts.graph.manifest import Manifest
from dbt.config import RuntimeConfig
from dbt.plugins.contracts import ExternalArtifact


def dbt_hook(func):
    setattr(func, "is_dbt_hook", True)
    return func


class dbtPlugin:
    def __init__(self):
        pass

    def build_external_nodes(self) -> List[ModelNodeArgs]:
        """TODO"""
        raise NotImplementedError

    def get_external_artifacts(
        self, manifest: Manifest, config: RuntimeConfig
    ) -> Dict[str, ExternalArtifact]:
        """TODO"""
        raise NotImplementedError


class PluginManager:
    PLUGIN_PREFIX = "dbt_"

    def __init__(self):
        discovered_dbt_modules = {
            name: importlib.import_module(name)
            for finder, name, ispkg in pkgutil.iter_modules()
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
        self, manifest: Manifest, config: RuntimeConfig
    ) -> Dict[str, ExternalArtifact]:
        external_artifacts = {}
        for hook_method in self.hooks.get("get_external_artifacts", []):
            plugin_external_artifact = hook_method(manifest, config)
            external_artifacts.update(plugin_external_artifact)
        return external_artifacts

    def build_external_nodes(self) -> List[ModelNodeArgs]:
        external_nodes = []
        for hook_method in self.hooks.get("build_external_nodes", []):
            plugin_external_nodes = hook_method()
            # TODO: ensure uniqueness
            external_nodes += plugin_external_nodes
        return external_nodes
