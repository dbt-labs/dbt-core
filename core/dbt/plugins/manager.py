import functools
import importlib
import logging
import pkgutil
from types import MappingProxyType
from typing import Callable, Dict, List, Mapping, NamedTuple, Sequence, Set, Tuple

import dbt.tracking
from dbt.contracts.graph.manifest import Manifest
from dbt.plugins.contracts import PluginArtifacts
from dbt.plugins.manifest import PluginNodes
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.tests import test_caching_enabled

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=4)
def _walk_prefixed_module_names(prefix: str) -> Tuple[str, ...]:
    """Cached walk of `pkgutil.iter_modules()` returning the names matching `prefix`.

    `pkgutil.iter_modules()` walks every entry on `sys.path`, which is slow in
    environments with many site-packages directories. The candidate name list is
    process-stable (the import path doesn't change between calls in a normal
    invocation), so we cache it process-wide. Opt-in signals are still evaluated
    per `get_prefixed_modules` call, so users who change env vars / flags between
    calls (e.g. via dbtRunner) still see the right behavior.

    Tests that mock `pkgutil.iter_modules` must clear this cache to avoid leaking
    fake results across cases -- the autouse fixture in
    tests/unit/plugins/test_manager.py does this."""
    return tuple(name for _, name, _ in pkgutil.iter_modules() if name.startswith(prefix))


class ManageSignal(NamedTuple):
    """How a single bundled plugin's load behavior is controlled.

    Read at PluginManager discovery time. The plugin is opt-in: it is only loaded
    when the flag resolves to True. Default is False -- absent any explicit signal,
    `importlib.import_module` is never called on the bundled module.

    - `flag_attr`: attribute on `get_flags()` (UPPERCASE). True means load the
      plugin; False (the default) means skip. Populated by the CLI parser from one
      of three surfaces, in precedence order:
        * explicit `--manage-state` / `--no-manage-state` on the command line
        * `DBT_ENGINE_MANAGE_STATE` env var (or its non-engine-prefixed alias
          `DBT_MANAGE_STATE`)
        * `manage_state` in dbt_project.yml's `flags:` block, or equivalently
          `manage_state` in profiles.yml's `config:` block
    - `cli_flag`: human-readable name of the corresponding CLI flag, used in log
      messages so a curious user knows what to pass to opt in.
    """

    flag_attr: str
    cli_flag: str


# Track which (module_name, reason) combinations have already emitted a discovery
# log message in this process, so we don't spam the user when PluginManager is
# constructed multiple times (e.g. during test runs that reuse the dbtRunner).
_DISCOVERY_NOTICES_EMITTED: Set[Tuple[str, str]] = set()


def _plugin_is_managed(flag_attr: str) -> bool:
    """Return True iff the user has explicitly opted in to loading this bundled plugin.

    Reads `get_flags().<flag_attr>` lazily to avoid an import-time dependency between
    PluginManager and the flags module. Defaults to False (skip) when flags haven't
    been initialized yet -- absent an explicit opt-in, the bundled plugin stays off."""
    try:
        from dbt.flags import get_flags

        return bool(getattr(get_flags(), flag_attr, False))
    except Exception:
        return False


def _notify_bundled_plugin_skipped(module_name: str, cli_flag: str) -> None:
    """Emit a one-time per-process DEBUG notice that a bundled plugin is installed
    on the import path but not loaded because the user hasn't opted in. Emitted at
    DEBUG (not INFO) because this is the default case -- every default invocation
    hits this path and shouldn't log INFO-level noise."""
    key = (module_name, "skipped")
    if key in _DISCOVERY_NOTICES_EMITTED:
        return
    _DISCOVERY_NOTICES_EMITTED.add(key)
    logger.debug(
        "Bundled plugin %r is installed but not loaded (opt-in); pass %s to enable.",
        module_name,
        cli_flag,
    )


def _notify_bundled_plugin_conflict(preferred: str, skipped: str) -> None:
    """Emit a one-time per-process warning when two modules from the same
    conflict group are simultaneously installed. We prefer the canonical
    (first-listed) module and skip the rest to avoid double monkey-patching."""
    key = (skipped, "conflict")
    if key in _DISCOVERY_NOTICES_EMITTED:
        return
    _DISCOVERY_NOTICES_EMITTED.add(key)
    logger.warning(
        "Both %r and %r are installed -- they are the same plugin under different "
        "package names. Loading %r and skipping %r to avoid double monkey-patching of "
        "core classes; uninstall the unused package to silence this warning.",
        preferred,
        skipped,
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

    # Bundled plugins that are installed as dependencies of dbt-core but are OPT-IN:
    # PluginManager only loads them when the user explicitly enables them. Each entry
    # maps a module name (as discovered by pkgutil) to a ManageSignal describing the
    # flag the user can flip to opt in.
    #
    # Both `dbt_state` and `dbt_run_cache` refer to the same plugin -- the package was
    # renamed from `run-cache` (module `dbt_run_cache`) to `dbt-state` (module
    # `dbt_state`). Either may be present in a user's environment depending on which
    # version they have installed, so both are listed here as first-class entries
    # sharing the same `manage_state` flag. Pass `--manage-state` (or
    # `DBT_ENGINE_MANAGE_STATE=true`, or `manage_state: true` in dbt_project.yml /
    # profiles.yml `config:`) to enable either module name.
    #
    # Skipping happens at module-discovery time (before importlib.import_module), so a
    # not-opted-in bundled plugin pays zero import cost and runs zero side effects --
    # even if its __init__.py has eager imports or monkey-patching.
    #
    # Scope: this gate only suppresses auto-discovery via pkgutil. If a user's project
    # code or another plugin explicitly `import dbt_state`s, the plugin's import-time
    # side effects will still fire. The plugin itself should also self-gate as
    # defense-in-depth; the registry here is the dbt-core-side contract.
    #
    # MappingProxyType makes the mapping read-only -- tampering raises TypeError rather
    # than silently flipping activation for the rest of the process.
    BUNDLED_PLUGIN_MODULES: Mapping[str, ManageSignal] = MappingProxyType(
        {
            "dbt_state": ManageSignal(flag_attr="MANAGE_STATE", cli_flag="--manage-state"),
            "dbt_run_cache": ManageSignal(flag_attr="MANAGE_STATE", cli_flag="--manage-state"),
        }
    )

    # Within a conflict group, if multiple modules are installed (e.g. both
    # `dbt_state` and `dbt_run_cache` happen to be present during the rename
    # transition), prefer the first listed and skip the rest with a warning. Avoids
    # non-deterministic double monkey-patching of CompileRunner/ModelRunner/etc.
    BUNDLED_PLUGIN_CONFLICT_GROUPS: Sequence[Sequence[str]] = (("dbt_state", "dbt_run_cache"),)

    @classmethod
    def _disabled_bundled_modules(cls) -> Set[str]:
        """Names of bundled plugin modules whose `manage_*` flag resolves to False."""
        return {
            module_name
            for module_name, signal in cls.BUNDLED_PLUGIN_MODULES.items()
            if not _plugin_is_managed(signal.flag_attr)
        }

    @classmethod
    def _resolve_bundled_plugin_conflicts(cls, candidate_names: Sequence[str]) -> Set[str]:
        """Return the set of module names that should be skipped due to conflict-group
        deduplication. Within each group, the first listed name that is present in
        `candidate_names` wins and the rest are returned as "skip me"."""
        candidates = set(candidate_names)
        skip: Set[str] = set()
        for group in cls.BUNDLED_PLUGIN_CONFLICT_GROUPS:
            present = [name for name in group if name in candidates]
            if len(present) <= 1:
                continue
            preferred, *rest = present
            for losing in rest:
                _notify_bundled_plugin_conflict(preferred, losing)
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
        # First pass: get the list of candidate module names without importing them.
        # The walk is cached process-wide via `_walk_prefixed_module_names` so
        # repeated PluginManager construction (e.g. multiple dbtRunner invocations
        # in the same process) doesn't re-scan sys.path each time.
        names_on_path = list(_walk_prefixed_module_names(cls.PLUGIN_MODULE_PREFIX))

        # For each bundled plugin on disk, check whether the user has opted in via
        # `--manage-state` / `DBT_ENGINE_MANAGE_STATE=true` / `manage_state: true`.
        # If not opted in, the module is filtered out before any import.
        disabled: Set[str] = set()
        for name in names_on_path:
            if name in cls.BUNDLED_PLUGIN_MODULES:
                signal = cls.BUNDLED_PLUGIN_MODULES[name]
                if not _plugin_is_managed(signal.flag_attr):
                    _notify_bundled_plugin_skipped(name, signal.cli_flag)
                    disabled.add(name)

        # Second pass: dedupe within conflict groups so we don't load two copies of
        # the same plugin under different package names. Only matters when both modules
        # are opted in -- the disabled-by-default path has already filtered them out.
        candidates = [n for n in names_on_path if n not in disabled]
        conflict_skips = cls._resolve_bundled_plugin_conflicts(candidates)
        to_import = [n for n in candidates if n not in conflict_skips]

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
