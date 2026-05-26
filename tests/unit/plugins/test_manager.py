from unittest import mock

import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.plugins import PluginManager, dbt_hook, dbtPlugin
from dbt.plugins.contracts import PluginArtifact, PluginArtifacts
from dbt.plugins.exceptions import dbtPluginError
from dbt.plugins.manager import _DISCOVERY_NOTICES_EMITTED, _plugin_is_managed
from dbt.plugins.manifest import ModelNodeArgs, PluginNodes


@pytest.fixture(autouse=True)
def _reset_discovery_notices():
    """The discovery notice de-dupe set is process-global; clear it between tests
    so a test that asserts on log emission isn't masked by an earlier test."""
    _DISCOVERY_NOTICES_EMITTED.clear()
    yield
    _DISCOVERY_NOTICES_EMITTED.clear()


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
    """Bundled plugins (e.g. dbt-state) load by default. The user can opt out via the
    `--no-manage-state` CLI flag, the DBT_ENGINE_MANAGE_STATE env var (handled by
    Click), or the `manage_state: false` project/profile flag -- all three resolve
    to `get_flags().MANAGE_STATE`. PluginManager skips the module at discovery time
    BEFORE `importlib.import_module` is called, so a disabled plugin pays zero
    import cost."""

    def test_plugin_is_managed_default_true(self):
        """`_plugin_is_managed` defaults to True if flags aren't initialized."""
        # No mocking -- exercises the real (defensive) default in the helper.
        # If `get_flags()` doesn't have MANAGE_STATE (or any other expected attr),
        # the helper must return True rather than silently skipping the plugin.
        with mock.patch("dbt.flags.get_flags", return_value=mock.MagicMock(spec=[])):
            assert _plugin_is_managed("MANAGE_STATE") is True

    def test_plugin_is_managed_reads_flags(self):
        fake_flags = mock.MagicMock()
        fake_flags.MANAGE_STATE = False
        with mock.patch("dbt.flags.get_flags", return_value=fake_flags):
            assert _plugin_is_managed("MANAGE_STATE") is False

        fake_flags.MANAGE_STATE = True
        with mock.patch("dbt.flags.get_flags", return_value=fake_flags):
            assert _plugin_is_managed("MANAGE_STATE") is True

    def test_plugin_is_managed_returns_true_on_exception(self):
        """If `get_flags()` itself raises (e.g. during very early startup), fall
        back to True -- never silently skip a bundled plugin."""
        with mock.patch("dbt.flags.get_flags", side_effect=RuntimeError("flags not ready")):
            assert _plugin_is_managed("MANAGE_STATE") is True

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

    def test_bundled_registry_is_read_only(self):
        # Wrapped in MappingProxyType so a buggy caller can't silently flip behavior
        # for the rest of the process.
        with pytest.raises(TypeError):
            PluginManager.BUNDLED_PLUGIN_MODULES["dbt_state"] = None  # type: ignore[index]
        with pytest.raises((TypeError, AttributeError)):
            PluginManager.BUNDLED_PLUGIN_MODULES.pop("dbt_state")  # type: ignore[attr-defined]

    def test_disabled_bundled_modules_empty_when_managed(self):
        """When `manage_state` is True, nothing is disabled -- bundled plugins
        load by default."""
        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=True):
            disabled = PluginManager._disabled_bundled_modules()
        assert disabled == set()

    def test_disabled_bundled_modules_when_unmanaged(self):
        """When `manage_state` is False (via CLI / env var / project flag), both
        module names are disabled."""
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

    def test_get_prefixed_modules_loads_by_default(self):
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

    def test_get_prefixed_modules_skips_when_unmanaged(self):
        """When manage_state is False, bundled plugin must be filtered out BEFORE
        `importlib.import_module` is called."""
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

    def test_discovery_logs_info_when_unmanaged(self):
        """Skipping should emit a one-time INFO mentioning the CLI flag the user can
        flip to re-enable."""
        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._plugin_is_managed", return_value=False), mock.patch(
            "dbt.plugins.manager.logger"
        ) as mock_logger, mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch(
            "dbt.plugins.manager.importlib.import_module"
        ):
            PluginManager.get_prefixed_modules()

        mock_logger.info.assert_called_once()
        args = mock_logger.info.call_args[0]
        assert "dbt_state" in args
        # The CLI flag name appears in the message so a user knows how to re-enable.
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

        assert mock_logger.info.call_count == 1

    def test_conflict_resolution_prefers_dbt_state_over_dbt_run_cache(self):
        """If both packages happen to be installed (mid-rename) and the user has NOT
        opted out, we must NOT load both -- double monkey-patching of
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

    def test_from_modules_instantiates_by_default(self):
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

    def test_from_modules_does_not_instantiate_when_unmanaged(self):
        """End-to-end contract: when manage_state is False (whatever the source --
        CLI / env / project / profile), the plugin is never instantiated."""
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

        assert instantiated == [], "unmanaged plugin must not be instantiated"
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
        """ProjectFlags' field default is None so the CLI option's default (True)
        wins when nothing is set in dbt_project.yml / profiles.yml."""
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
