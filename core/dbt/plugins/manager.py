import importlib
import logging
import os
import pkgutil
from types import MappingProxyType
from typing import Callable, Dict, List, Mapping, Sequence, Set, Tuple

import dbt.tracking
from dbt.contracts.graph.manifest import Manifest
from dbt.plugins.contracts import PluginArtifacts
from dbt.plugins.manifest import PluginNodes
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.tests import test_caching_enabled

logger = logging.getLogger(__name__)

# Values that count as "on" when reading a gate env var. The canonical seven; we deliberately
# don't accept variants like "enable"/"enabled" -- if users ask, this is the documented list.
_TRUTHY_ENV_VALUES = frozenset({"true", "1", "t", "y", "yes", "on"})


def _env_gate_is_on(env_var: str) -> bool:
    """Return True iff `env_var` is set to one of the recognized truthy values."""
    raw = os.environ.get(env_var)
    if raw is None:
        return False
    return raw.strip().lower() in _TRUTHY_ENV_VALUES


# Track which (module_name, gate_state) combinations have already emitted a discovery
# log message in this process, so we don't spam the user when PluginManager is
# constructed multiple times (e.g. during test runs that reuse the dbtRunner).
_DISCOVERY_NOTICES_EMITTED: Set[Tuple[str, str]] = set()


def _notify_opt_in_module_skipped(module_name: str, env_var: str) -> None:
    """Emit a one-time per-process notice that an opt-in plugin module was found on the
    import path but skipped because its gate is off.

    The legacy `dbt_run_cache` name is logged at INFO -- users on pre-bundling versions had
    that plugin auto-loading, so the silent behavior change deserves visibility. The new
    `dbt_state` name is logged at DEBUG: there's no pre-existing behavior to break, but
    operators may still want a breadcrumb when debugging."""
    key = (module_name, "skipped")
    if key in _DISCOVERY_NOTICES_EMITTED:
        return
    _DISCOVERY_NOTICES_EMITTED.add(key)
    msg = (
        "Plugin module %r was found on the import path but %s is not set; "
        "the plugin will not be loaded. Set %s=true to enable."
    )
    if module_name == "dbt_run_cache":
        # Silent behavior change risk: this name auto-loaded in older dbt-core versions.
        logger.info(msg, module_name, env_var, env_var)
    else:
        logger.debug(msg, module_name, env_var, env_var)


def _notify_opt_in_conflict(preferred: str, skipped: str, env_var: str) -> None:
    """Emit a one-time per-process warning when two modules from the same opt-in
    conflict group are simultaneously enabled by the gate. We prefer the canonical
    (first-listed) module and skip the rest to avoid double monkey-patching."""
    key = (skipped, "conflict")
    if key in _DISCOVERY_NOTICES_EMITTED:
        return
    _DISCOVERY_NOTICES_EMITTED.add(key)
    logger.warning(
        "Both %r and %r are installed and %s is set. They are the same plugin under "
        "different package names. Loading %r and skipping %r to avoid double "
        "monkey-patching of core classes -- uninstall the unused package to silence "
        "this warning.",
        preferred,
        skipped,
        env_var,
        preferred,
        skipped,
    )


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


# Module-level cache used by `from_modules` when test caching is enabled. The gate is
# read at discovery time and the result is cached here; tests that toggle a gate env
# var mid-process need to clear this cache to see the change.
_MODULES_CACHE = None


class PluginManager:
    PLUGIN_MODULE_PREFIX = "dbt_"
    PLUGIN_ATTR_NAME = "plugins"

    # Bundled plugins that are installed as dependencies of dbt-core but must not be
    # discovered or initialized unless their gate env var is set to a truthy value.
    # Both `dbt_state` and `dbt_run_cache` refer to the same plugin -- the package was
    # renamed from `run-cache` (module `dbt_run_cache`) to `dbt-state` (module
    # `dbt_state`). Either may be present in a user's environment depending on which
    # version they have installed, so both are listed here as first-class entries
    # gated by the same env var. Setting DBT_ENGINE_STATE_ENABLED=true opts in regardless
    # of which module name pkgutil discovers.
    #
    # Skipping happens at module-discovery time (before importlib.import_module), so a
    # disabled opt-in plugin pays zero import cost and runs zero side effects -- even
    # if its __init__.py has eager imports or monkey-patching.
    #
    # Scope: this gate only suppresses auto-discovery via pkgutil. If a user's project
    # code or another plugin explicitly `import dbt_state`s, the plugin's import-time
    # side effects will still fire. The plugin itself should also self-gate as
    # defense-in-depth; the registry here is the dbt-core-side contract.
    #
    # MappingProxyType makes the mapping read-only -- tampering raises TypeError rather
    # than silently flipping activation for the rest of the process.
    OPT_IN_PLUGIN_MODULES: Mapping[str, str] = MappingProxyType(
        {
            "dbt_state": "DBT_ENGINE_STATE_ENABLED",
            "dbt_run_cache": "DBT_ENGINE_STATE_ENABLED",
        }
    )

    # Within a conflict group, if multiple modules pass the gate (e.g. both
    # `dbt_state` and `dbt_run_cache` happen to be installed during the rename
    # transition), prefer the first listed and skip the rest with a warning. Avoids
    # non-deterministic double monkey-patching of CompileRunner/ModelRunner/etc.
    OPT_IN_PLUGIN_CONFLICT_GROUPS: Sequence[Sequence[str]] = (("dbt_state", "dbt_run_cache"),)

    @classmethod
    def _disabled_opt_in_modules(cls) -> Set[str]:
        """Names of opt-in plugin modules whose gate env var is NOT set to a truthy value."""
        return {
            module_name
            for module_name, env_var in cls.OPT_IN_PLUGIN_MODULES.items()
            if not _env_gate_is_on(env_var)
        }

    @classmethod
    def _resolve_opt_in_conflicts(cls, candidate_names: Sequence[str]) -> Set[str]:
        """Return the set of module names that should be skipped due to conflict-group
        deduplication. Within each group, the first listed name that is present in
        `candidate_names` wins and the rest are returned as "skip me"."""
        candidates = set(candidate_names)
        skip: Set[str] = set()
        for group in cls.OPT_IN_PLUGIN_CONFLICT_GROUPS:
            present = [name for name in group if name in candidates]
            if len(present) <= 1:
                continue
            preferred, *rest = present
            for losing in rest:
                env_var = cls.OPT_IN_PLUGIN_MODULES.get(losing, "")
                _notify_opt_in_conflict(preferred, losing, env_var)
                skip.add(losing)
        return skip

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
        disabled = cls._disabled_opt_in_modules()

        # First pass: walk iter_modules without importing, so we can spot opt-in
        # modules that are physically present on disk and emit the "found but gated
        # off" notice for the ones we're about to skip. This is what gives existing
        # `dbt_run_cache` users a breadcrumb when their previously-auto-loaded plugin
        # stops loading.
        names_on_path = [
            name
            for _, name, _ in pkgutil.iter_modules()
            if name.startswith(cls.PLUGIN_MODULE_PREFIX)
        ]
        for name in names_on_path:
            if name in disabled:
                _notify_opt_in_module_skipped(name, cls.OPT_IN_PLUGIN_MODULES[name])

        # Second pass: for modules whose gate is ON, dedupe within conflict groups.
        enabled_candidates = [n for n in names_on_path if n not in disabled]
        conflict_skips = cls._resolve_opt_in_conflicts(enabled_candidates)
        to_import = [n for n in enabled_candidates if n not in conflict_skips]

        return {name: importlib.import_module(name) for name in to_import}

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
