import functools
import importlib
import pkgutil
from types import ModuleType
from typing import Callable, Dict, List, Mapping

import dbt.tracking
from dbt.contracts.graph.manifest import Manifest
from dbt.plugins.contracts import PluginArtifacts
from dbt.plugins.manifest import PluginNodes
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.tests import test_caching_enabled


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

    def __init__(self, project_name: str) -> None:
        self.project_name = project_name
        try:
            self.initialize()
        except DbtRuntimeError as e:
            # Remove the first line of DbtRuntimeError to avoid redundant "Runtime Error" line
            raise DbtRuntimeError("\n".join(str(e).split("\n")[1:]))
        except Exception as e:
            raise DbtRuntimeError(str(e))

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


@functools.lru_cache(maxsize=None)
def _get_dbt_modules() -> Mapping[str, ModuleType]:
    # This is an expensive function, especially in the context of testing, when
    # it is called repeatedly, so we break it out and cache the result globally.
    return {
        name: importlib.import_module(name)
        for _, name, _ in pkgutil.iter_modules()
        if name.startswith(PluginManager.PLUGIN_MODULE_PREFIX)
    }


_MODULES_CACHE = None


class PluginManager:
    PLUGIN_MODULE_PREFIX = "dbt_"
    PLUGIN_ATTR_NAME = "plugins"

    def __init__(self, plugins: List[dbtPlugin]) -> None:
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

        if test_caching_enabled():
            global _MODULES_CACHE
            if _MODULES_CACHE is None:
                discovered_dbt_modules = cls.get_prefixed_modules()
                _MODULES_CACHE = discovered_dbt_modules
            else:
                discovered_dbt_modules = _MODULES_CACHE
        else:
            discovered_dbt_modules = cls.get_prefixed_modules()

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

    @classmethod
    def get_prefixed_modules(cls):
        return {
            name: importlib.import_module(name)
            for _, name, _ in pkgutil.iter_modules()
            if name.startswith(cls.PLUGIN_MODULE_PREFIX)
        }

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
            dbt.tracking.track_plugin_get_nodes(
                {
                    "plugin_name": hook_method.__self__.name,  # type: ignore
                    "num_model_nodes": len(plugin_nodes.models),
                    "num_model_packages": len(
                        {model.package_name for model in plugin_nodes.models.values()}
                    ),
                }
            )
            all_plugin_nodes.update(plugin_nodes)
        return all_plugin_nodes
