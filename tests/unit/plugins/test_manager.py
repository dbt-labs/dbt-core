import os
from unittest import mock

import pytest

import dbt.plugins.manager
from dbt.exceptions import DbtRuntimeError
from dbt.plugins import PluginManager, dbt_hook, dbtPlugin
from dbt.plugins.contracts import PluginArtifact, PluginArtifacts
from dbt.plugins.exceptions import dbtPluginError
from dbt.plugins.manager import (
    _DISCOVERY_NOTICES_EMITTED,
    _plugin_is_managed,
    _walk_prefixed_module_names,
)
from dbt.plugins.manifest import ModelNodeArgs, PluginNodes


@pytest.fixture(autouse=True)
def _reset_plugin_manager_process_state():
    """PluginManager has three pieces of process-global state that can leak between
    tests if not cleared:

    1. `_DISCOVERY_NOTICES_EMITTED` -- the de-dupe set for one-time log notices.
       A test that asserts on log emission would be masked by an earlier test
       that already emitted the notice.
    2. `_MODULES_CACHE` -- the `from_modules` cache that's active when
       `test_caching_enabled()` returns True. A test that mocks
       `pkgutil.iter_modules` would silently see another test's cached dict
       instead of its own mock results -- making assertions pass for the wrong
       reason (or fail unpredictably depending on ordering).
    3. The `_walk_prefixed_module_names` lru_cache -- caches the candidate name
       list at process scope. Tests that mock `pkgutil.iter_modules` would see
       the cached pre-mock walk on subsequent calls.

    Clear all three before AND after each test so ordering doesn't matter."""

    def _clear() -> None:
        _DISCOVERY_NOTICES_EMITTED.clear()
        dbt.plugins.manager._MODULES_CACHE = None
        _walk_prefixed_module_names.cache_clear()

    _clear()
    yield
    _clear()


class ExceptionInitializePlugin(dbtPlugin):
    def initialize(self) -> None:
        raise Exception("plugin error message")


class dbtRuntimeErrorInitializePlugin(dbtPlugin):
    def initialize(self) -> None:
        raise dbtPluginError("plugin error message")


class GetNodesPlugin(dbtPlugin):
    @dbt_hook
    def get_nodes(self) -> PluginNodes:
        nodes = PluginNodes()
        nodes.add_model(
            ModelNodeArgs(
                name="test_name",
                package_name=self.project_name,
                identifier="test_identifier",
                schema="test_schema",
            )
        )
        return nodes


class GetArtifactsPlugin(dbtPlugin):
    @dbt_hook
    def get_manifest_artifacts(self, manifest) -> PluginArtifacts:
        return {self.project_name: PluginArtifact()}


class TestPluginManager:
    @pytest.fixture
    def get_nodes_plugin(self):
        return GetNodesPlugin(project_name="test")

    @pytest.fixture
    def get_nodes_plugins(self, get_nodes_plugin):
        return [get_nodes_plugin, GetNodesPlugin(project_name="test2")]

    @pytest.fixture
    def get_artifacts_plugin(self):
        return GetArtifactsPlugin(project_name="test")

    @pytest.fixture
    def get_artifacts_plugins(self, get_artifacts_plugin):
        return [get_artifacts_plugin, GetArtifactsPlugin(project_name="test2")]

    def test_plugin_manager_init_exception(self):
        with pytest.raises(DbtRuntimeError, match="plugin error message"):
            PluginManager(plugins=[ExceptionInitializePlugin(project_name="test")])

    def test_plugin_manager_init_plugin_exception(self):
        with pytest.raises(DbtRuntimeError, match="^Runtime Error\n    plugin error message"):
            PluginManager(plugins=[dbtRuntimeErrorInitializePlugin(project_name="test")])

    def test_plugin_manager_init_single_hook(self, get_nodes_plugin):
        pm = PluginManager(plugins=[get_nodes_plugin])
        assert len(pm.hooks) == 1

        assert "get_nodes" in pm.hooks
        assert len(pm.hooks["get_nodes"]) == 1
        assert pm.hooks["get_nodes"][0] == get_nodes_plugin.get_nodes

    def test_plugin_manager_init_single_hook_multiple_methods(self, get_nodes_plugins):
        pm = PluginManager(plugins=get_nodes_plugins)
        assert len(pm.hooks) == 1

        assert "get_nodes" in pm.hooks
        assert len(pm.hooks["get_nodes"]) == 2
        assert pm.hooks["get_nodes"][0] == get_nodes_plugins[0].get_nodes
        assert pm.hooks["get_nodes"][1] == get_nodes_plugins[1].get_nodes

    def test_plugin_manager_init_multiple_hooks(self, get_nodes_plugin, get_artifacts_plugin):
        pm = PluginManager(plugins=[get_nodes_plugin, get_artifacts_plugin])
        assert len(pm.hooks) == 2

        assert "get_nodes" in pm.hooks
        assert len(pm.hooks["get_nodes"]) == 1
        assert pm.hooks["get_nodes"][0] == get_nodes_plugin.get_nodes

        assert "get_manifest_artifacts" in pm.hooks
        assert len(pm.hooks["get_manifest_artifacts"]) == 1
        assert pm.hooks["get_manifest_artifacts"][0] == get_artifacts_plugin.get_manifest_artifacts

    @mock.patch("dbt.tracking")
    def test_get_nodes(self, tracking, get_nodes_plugins):
        tracking.active_user = mock.Mock()
        pm = PluginManager(plugins=get_nodes_plugins)

        nodes = pm.get_nodes()

        assert len(nodes.models) == 2

        expected_calls = [
            mock.call(
                {
                    "plugin_name": get_nodes_plugins[0].name,
                    "num_model_nodes": 1,
                    "num_model_packages": 1,
                }
            ),
            mock.call(
                {
                    "plugin_name": get_nodes_plugins[1].name,
                    "num_model_nodes": 1,
                    "num_model_packages": 1,
                }
            ),
        ]

        tracking.track_plugin_get_nodes.assert_has_calls(expected_calls)

    def test_get_manifest_artifact(self, get_artifacts_plugins):
        pm = PluginManager(plugins=get_artifacts_plugins)
        artifacts = pm.get_manifest_artifacts(None)
        assert len(artifacts) == 2


class TestBundledPluginGating:
    """Bundled plugins (e.g. dbt-state) are opt-in. The user enables them via the
    `--manage-state` CLI flag, the DBT_ENGINE_MANAGE_STATE env var (handled by
    Click), or the `manage_state: true` project/profile flag -- all three resolve
    to `get_flags().MANAGE_STATE`. PluginManager skips the module at discovery time
    BEFORE `importlib.import_module` is called, so a not-opted-in plugin pays zero
    import cost."""

    def test_plugin_is_managed_default_false(self):
        """`_plugin_is_managed` defaults to False when the flag attribute is missing.

        Opt-in default: absent an explicit opt-in via flags, the helper returns
        False so PluginManager skips the bundled plugin."""
        with mock.patch("dbt.flags.get_flags", return_value=mock.MagicMock(spec=[])):
            assert _plugin_is_managed("MANAGE_STATE") is False

    def test_plugin_is_managed_reads_flags(self):
        fake_flags = mock.MagicMock()
        fake_flags.MANAGE_STATE = False
        with mock.patch("dbt.flags.get_flags", return_value=fake_flags):
            assert _plugin_is_managed("MANAGE_STATE") is False

        fake_flags.MANAGE_STATE = True
        with mock.patch("dbt.flags.get_flags", return_value=fake_flags):
            assert _plugin_is_managed("MANAGE_STATE") is True

    def test_plugin_is_managed_returns_false_on_exception(self):
        """If `get_flags()` itself raises (e.g. during very early startup), fall
        back to False -- opt-in plugins stay off unless explicitly enabled."""
        with mock.patch("dbt.flags.get_flags", side_effect=RuntimeError("flags not ready")):
            assert _plugin_is_managed("MANAGE_STATE") is False

    def test_bundled_registry_contains_both_module_names(self):
        """dbt-state is bundled with dbt-core and loads by default. Both `dbt_state`
        (new package name) and `dbt_run_cache` (legacy name from before the rename)
        refer to the same plugin and share the same manage signal."""
        signal_state = PluginManager.BUNDLED_PLUGIN_MODULES["dbt_state"]
        assert signal_state.flag_attr == "MANAGE_STATE"
        assert signal_state.cli_flag == "--manage-state"

        signal_run_cache = PluginManager.BUNDLED_PLUGIN_MODULES["dbt_run_cache"]
        assert signal_run_cache.flag_attr == "MANAGE_STATE"
        assert signal_run_cache.cli_flag == "--manage-state"

    def test_walk_prefixed_module_names_is_cached(self):
        """`_walk_prefixed_module_names` must cache the `pkgutil.iter_modules()`
        walk so repeated PluginManager construction doesn't re-scan sys.path.
        Multiple calls with the same prefix should hit `iter_modules` exactly once."""
        call_count = 0

        def counting_iter():
            nonlocal call_count
            call_count += 1
            return iter([(None, "dbt_state", False), (None, "dbt_other", False)])

        with mock.patch("dbt.plugins.manager.pkgutil.iter_modules", side_effect=counting_iter):
            r1 = _walk_prefixed_module_names("dbt_")
            r2 = _walk_prefixed_module_names("dbt_")
            r3 = _walk_prefixed_module_names("dbt_")

        assert call_count == 1, "iter_modules should be invoked exactly once"
        assert r1 == r2 == r3 == ("dbt_state", "dbt_other")

    def test_bundled_registry_is_read_only(self):
        # Wrapped in MappingProxyType so a buggy caller can't silently flip behavior
        # for the rest of the process.
        with pytest.raises(TypeError):
            PluginManager.BUNDLED_PLUGIN_MODULES["dbt_state"] = None  # type: ignore[index]
        with pytest.raises((TypeError, AttributeError)):
            PluginManager.BUNDLED_PLUGIN_MODULES.pop("dbt_state")  # type: ignore[attr-defined]

    def test_disabled_bundled_modules_empty_when_opted_in(self):
        """When `manage_state` is True (user has opted in), nothing is disabled --
        bundled plugins load."""
        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=True):
            disabled = PluginManager._disabled_bundled_modules()
        assert disabled == set()

    def test_disabled_bundled_modules_when_not_opted_in(self):
        """When `manage_state` is False (the default), both module names are
        disabled."""
        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=False):
            disabled = PluginManager._disabled_bundled_modules()
        assert "dbt_state" in disabled
        assert "dbt_run_cache" in disabled

    # NOTE on patch ordering in the tests below: `mock.patch` resolves its target via
    # `importlib.import_module`. If we patch `dbt.plugins.manager.importlib.import_module`
    # FIRST, any subsequent patch on `dbt.plugins.manager.<attr>` in the same with-block
    # silently fails -- mock.patch's own resolver hits the mocked import_module and gets a
    # MagicMock back instead of the real module. So `logger` / `_notify_*` /
    # `_plugin_is_managed` patches go first; `importlib.import_module` last.

    def test_get_prefixed_modules_loads_when_opted_in(self):
        """When manage_state is True, bundled plugin imports normally."""
        fake_iter_result = [
            (None, "dbt_state", False),
            (None, "dbt_someother", False),
            (None, "unrelated_pkg", False),
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=True), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" in modules
        assert "dbt_someother" in modules
        assert "unrelated_pkg" not in modules
        assert "dbt_state" in imported

    def test_get_prefixed_modules_skips_by_default(self):
        """When manage_state is False (the default), bundled plugin must be filtered
        out BEFORE `importlib.import_module` is called."""
        fake_iter_result = [
            (None, "dbt_state", False),
            (None, "dbt_someother", False),
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=False), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" not in modules
        assert "dbt_someother" in modules
        # Zero-cost: the disabled plugin is never imported.
        assert "dbt_state" not in imported

    def test_discovery_logs_debug_when_not_opted_in(self):
        """Skipping should emit a one-time DEBUG mentioning the CLI flag the user
        can pass to opt in. DEBUG (not INFO) because the not-opted-in state is the
        default and shouldn't log INFO-level noise on every default invocation."""
        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=False), mock.patch(
            "dbt.plugins.manager.logger"
        ) as mock_logger, mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch(
            "dbt.plugins.manager.importlib.import_module"
        ):
            PluginManager.get_prefixed_modules()

        mock_logger.debug.assert_called_once()
        mock_logger.info.assert_not_called()
        args = mock_logger.debug.call_args[0]
        assert "dbt_state" in args
        # The CLI flag name appears in the message so a curious user knows how to opt in.
        assert any("--manage-state" in str(a) for a in args)

    def test_discovery_notice_is_emitted_only_once_per_process(self):
        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=False), mock.patch(
            "dbt.plugins.manager.logger"
        ) as mock_logger, mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch(
            "dbt.plugins.manager.importlib.import_module"
        ):
            PluginManager.get_prefixed_modules()
            PluginManager.get_prefixed_modules()
            PluginManager.get_prefixed_modules()

        assert mock_logger.debug.call_count == 1

    def test_conflict_resolution_prefers_dbt_state_over_dbt_run_cache(self):
        """If both packages happen to be installed (mid-rename) and the user has
        opted in, we must NOT load both -- double monkey-patching of
        CompileRunner/ModelRunner is non-deterministic. Prefer the canonical name
        and warn about the loser."""
        fake_iter_result = [
            (None, "dbt_state", False),
            (None, "dbt_run_cache", False),
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=True), mock.patch(
            "dbt.plugins.manager.logger"
        ) as mock_logger, mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch(
            "dbt.plugins.manager.importlib.import_module", side_effect=fake_import
        ):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" in imported
        assert "dbt_run_cache" not in imported
        assert "dbt_state" in modules
        assert "dbt_run_cache" not in modules
        mock_logger.warning.assert_called_once()
        warn_args = mock_logger.warning.call_args[0]
        assert "dbt_state" in warn_args and "dbt_run_cache" in warn_args

    def test_from_modules_instantiates_when_opted_in(self):
        """End-to-end contract: with manage_state True, the bundled plugin is
        instantiated."""
        instantiated = []

        class _FakeStatePlugin(dbtPlugin):
            def initialize(self):
                instantiated.append("dbt_state")

        fake_state_module = mock.MagicMock()
        fake_state_module.plugins = [_FakeStatePlugin]

        def fake_import(name):
            if name == "dbt_state":
                return fake_state_module
            raise ImportError(name)

        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=True), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            PluginManager.from_modules(project_name="test")

        assert instantiated == ["dbt_state"]

    def test_from_modules_does_not_instantiate_by_default(self):
        """End-to-end contract: when manage_state is False (the default; user has
        not opted in via CLI / env / project / profile), the plugin is never
        instantiated."""
        instantiated = []

        class _FakeStatePlugin(dbtPlugin):
            def initialize(self):
                instantiated.append("dbt_state")

        fake_state_module = mock.MagicMock()
        fake_state_module.plugins = [_FakeStatePlugin]

        def fake_import(name):
            if name == "dbt_state":
                return fake_state_module
            raise ImportError(name)

        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=False), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            pm = PluginManager.from_modules(project_name="test")

        assert instantiated == [], "opt-in plugin must not be instantiated by default"
        assert pm.hooks == {}


class TestProjectFlagsExposeManageState:
    """The `manage_state` flag must be wired through ProjectFlags so that
    dbt_project.yml's `flags:` block and profiles.yml's `config:` block both work
    (they share the same parser via `read_project_flags`). Because manage_state is
    also backed by the `--manage-state` CLI option, the field lives in the regular
    ProjectFlags section (NOT in project_only_flags) -- cli/flags.py reads it via
    `params_assigned_from_default` to override the CLI default when not explicitly
    passed."""

    def test_default_is_none(self):
        """ProjectFlags' field default is None so the CLI option's default (False)
        wins when nothing is set in dbt_project.yml / profiles.yml. Opt-in by
        default means: absent any explicit signal, the plugin doesn't load."""
        from dbt.contracts.project import ProjectFlags

        assert ProjectFlags().manage_state is None

    def test_can_be_set_from_dict(self):
        from dbt.contracts.project import ProjectFlags

        pf = ProjectFlags.from_dict({"manage_state": False})
        assert pf.manage_state is False

        pf = ProjectFlags.from_dict({"manage_state": True})
        assert pf.manage_state is True

    def test_is_not_in_project_only_flags(self):
        """manage_state has a CLI option backing it, so it must NOT be in
        project_only_flags -- otherwise it would be set both via the regular
        CLI-default-override path AND the project-only path, and the two could
        disagree."""
        from dbt.contracts.project import ProjectFlags

        pf = ProjectFlags(manage_state=False)
        assert "manage_state" not in pf.project_only_flags


class TestManageStateClickIntegration:
    """End-to-end: --manage-state on the CLI, DBT_ENGINE_MANAGE_STATE env var, and
    manage_state in ProjectFlags (read from dbt_project.yml's `flags:` block or
    profiles.yml's `config:` block) must each enable the plugin on their own.
    These tests exercise the real Click parser and the real Flags constructor
    (no mocks of _plugin_is_managed / get_flags) so a future refactor of the
    Click-to-Flags pipeline can't silently break any of the opt-in paths."""

    @staticmethod
    def _probe_manage_state(argv, env, project_flags=None):
        """Run a minimal Click command that applies the @p.manage_state decorator,
        construct the Flags object from the resulting context (with the supplied
        ProjectFlags injected, bypassing disk I/O), and return what
        `_plugin_is_managed("MANAGE_STATE")` would see."""
        import click
        from click.testing import CliRunner

        import dbt.flags
        from dbt.cli import params as p
        from dbt.cli.flags import Flags
        from dbt.plugins.manager import _plugin_is_managed

        results = {}

        @click.command()
        @p.manage_state
        @click.pass_context
        def probe(ctx, **kwargs):
            flags = Flags(ctx, project_flags=project_flags)
            dbt.flags.set_flags(flags)
            results["ctx_param"] = ctx.params.get("manage_state")
            results["flags_attr"] = getattr(flags, "MANAGE_STATE", None)
            results["plugin_is_managed"] = _plugin_is_managed("MANAGE_STATE")

        runner = CliRunner()
        result = runner.invoke(probe, argv, env=env, catch_exceptions=False)
        assert result.exit_code == 0, result.output
        return results

    def test_cli_flag_alone_enables_plugin(self, monkeypatch):
        """--manage-state supplied, DBT_ENGINE_MANAGE_STATE NOT set, no project
        flag -> plugin enabled."""
        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.delenv("DBT_MANAGE_STATE", raising=False)

        result = self._probe_manage_state(["--manage-state"], env={})
        assert result["ctx_param"] is True
        assert result["flags_attr"] is True
        assert result["plugin_is_managed"] is True

    def test_env_var_alone_enables_plugin(self, monkeypatch):
        """Env var supplied, no CLI flag, no project flag -> plugin enabled."""
        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        result = self._probe_manage_state([], env={"DBT_ENGINE_MANAGE_STATE": "true"})
        assert result["plugin_is_managed"] is True

    def test_project_flag_alone_enables_plugin(self, monkeypatch):
        """ProjectFlags(manage_state=True) supplied, no CLI flag, no env var ->
        plugin enabled. Covers `flags.manage_state: true` in dbt_project.yml and
        the equivalent `config.manage_state: true` in profiles.yml -- both
        surfaces feed into the same ProjectFlags via `read_project_flags`."""
        from dbt.contracts.project import ProjectFlags

        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.delenv("DBT_MANAGE_STATE", raising=False)

        result = self._probe_manage_state(
            [], env={}, project_flags=ProjectFlags(manage_state=True)
        )
        assert result["flags_attr"] is True
        assert result["plugin_is_managed"] is True

    def test_project_flag_false_keeps_plugin_off(self, monkeypatch):
        """Symmetric: ProjectFlags(manage_state=False) with no other signal ->
        plugin skipped (same as the no-signal default)."""
        from dbt.contracts.project import ProjectFlags

        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.delenv("DBT_MANAGE_STATE", raising=False)

        result = self._probe_manage_state(
            [], env={}, project_flags=ProjectFlags(manage_state=False)
        )
        assert result["plugin_is_managed"] is False

    def test_cli_flag_overrides_project_flag(self, monkeypatch):
        """Precedence: explicit CLI --no-manage-state beats project
        manage_state: true. cli/flags.py only lets project_flags override values
        that came from the CLI default; explicit CLI values stick."""
        from dbt.contracts.project import ProjectFlags

        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.delenv("DBT_MANAGE_STATE", raising=False)

        result = self._probe_manage_state(
            ["--no-manage-state"], env={}, project_flags=ProjectFlags(manage_state=True)
        )
        assert result["plugin_is_managed"] is False

    def test_cli_no_flag_overrides_env_var_true(self, monkeypatch):
        """Precedence: explicit CLI --no-manage-state beats DBT_ENGINE_MANAGE_STATE=true.
        Click's resolution order is CLI > env var > default; the env var only
        flips the source from DEFAULT to ENVIRONMENT when nothing's on the CLI."""
        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.delenv("DBT_MANAGE_STATE", raising=False)

        result = self._probe_manage_state(
            ["--no-manage-state"], env={"DBT_ENGINE_MANAGE_STATE": "true"}
        )
        assert result["plugin_is_managed"] is False

    def test_cli_flag_overrides_env_var_false(self, monkeypatch):
        """Symmetric precedence at the PluginManager level: explicit CLI
        --manage-state beats DBT_ENGINE_MANAGE_STATE=false. Same Click rule as
        the inverse: explicit CLI value wins regardless of the env var.

        NOTE: this only checks PluginManager's view. See
        `test_cli_override_normalizes_env_var_for_plugin` below for the
        critical end-to-end check that the plugin's own initialize() also
        sees the resolved value."""
        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.delenv("DBT_MANAGE_STATE", raising=False)

        result = self._probe_manage_state(
            ["--manage-state"], env={"DBT_ENGINE_MANAGE_STATE": "false"}
        )
        assert result["plugin_is_managed"] is True

    def test_cli_override_normalizes_env_var_for_plugin(self, monkeypatch):
        """Real bug: bundled plugins read DBT_ENGINE_MANAGE_STATE directly in
        their initialize() (rather than going through `get_flags()`). When the
        user passes `--manage-state` while the env var is set to "false",
        PluginManager correctly decides to load the plugin -- but if `os.environ`
        still says "false", the plugin self-disables on import.

        PluginManager normalizes `os.environ[env_var]` to "true" right before
        importing each bundled plugin, so the plugin's view agrees with the
        Click-resolved value. This test simulates exactly what real dbt-state
        does: a plugin that reads the env var in its initialize()."""
        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.setenv("DBT_ENGINE_MANAGE_STATE", "false")

        # The plugin's own self-check: what real dbt-state does.
        seen_by_plugin = {}

        class _FakeBundledPlugin(dbtPlugin):
            def initialize(self):
                # Read directly from os.environ, as real dbt-state does.
                seen_by_plugin["env_var"] = os.environ.get("DBT_ENGINE_MANAGE_STATE")

        fake_module = mock.MagicMock()
        fake_module.plugins = [_FakeBundledPlugin]

        def fake_import(name):
            if name == "dbt_state":
                return fake_module
            raise ImportError(name)

        fake_iter_result = [(None, "dbt_state", False)]

        # PluginManager must see manage_state=True (CLI override).
        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=True), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            PluginManager.from_modules(project_name="test")

        # Plugin's initialize() must have seen "true", not the original "false".
        assert seen_by_plugin["env_var"] == "true", (
            f"Plugin saw DBT_ENGINE_MANAGE_STATE={seen_by_plugin['env_var']!r}; "
            "expected 'true' (PluginManager should have normalized os.environ "
            "before plugin import so the plugin's own env-var check agrees "
            "with the Click-resolved value)."
        )

    def test_neither_flag_nor_env_var_skips_plugin(self, monkeypatch):
        """Default opt-in-off behavior: nothing set anywhere -> plugin skipped."""
        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        monkeypatch.delenv("DBT_MANAGE_STATE", raising=False)

        result = self._probe_manage_state([], env={})
        assert result["plugin_is_managed"] is False

    def test_no_manage_state_flag_skips_plugin(self, monkeypatch):
        """Explicit --no-manage-state -> plugin skipped."""
        monkeypatch.delenv("DBT_ENGINE_MANAGE_STATE", raising=False)
        result = self._probe_manage_state(["--no-manage-state"], env={})
        assert result["plugin_is_managed"] is False
