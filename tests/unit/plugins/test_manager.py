from unittest import mock

import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.plugins import PluginManager, dbt_hook, dbtPlugin
from dbt.plugins.contracts import PluginArtifact, PluginArtifacts
from dbt.plugins.exceptions import dbtPluginError
from dbt.plugins.manager import _DISCOVERY_NOTICES_EMITTED, _env_var_is_truthy
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


class TestOptOutPluginGating:
    """Bundled plugins (e.g. dbt-state) load by default. The user can opt out via env
    var OR project flag; PluginManager skips the module at discovery time BEFORE
    `importlib.import_module` is called, so a disabled plugin pays zero import cost."""

    def test_env_var_unset(self, monkeypatch):
        monkeypatch.delenv("DBT_TEST_GATE", raising=False)
        assert _env_var_is_truthy("DBT_TEST_GATE") is False

    @pytest.mark.parametrize(
        "value", ["true", "True", "TRUE", "1", "t", "y", "yes", "on", " on ", "ON"]
    )
    def test_env_var_truthy_values(self, monkeypatch, value):
        monkeypatch.setenv("DBT_TEST_GATE", value)
        assert _env_var_is_truthy("DBT_TEST_GATE") is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "disabled", "anything"])
    def test_env_var_non_truthy_values(self, monkeypatch, value):
        monkeypatch.setenv("DBT_TEST_GATE", value)
        assert _env_var_is_truthy("DBT_TEST_GATE") is False

    def test_opt_out_registry_contains_both_module_names(self):
        """dbt-state is bundled with dbt-core and loads by default. Both `dbt_state`
        (new package name) and `dbt_run_cache` (legacy name from before the rename)
        refer to the same plugin and share the same opt-out signal."""
        signal_state = PluginManager.OPT_OUT_PLUGIN_MODULES["dbt_state"]
        assert signal_state.env_var == "DBT_ENGINE_STATE_DISABLED"
        assert signal_state.flag_attr == "STATE_PLUGIN_DISABLED"

        signal_run_cache = PluginManager.OPT_OUT_PLUGIN_MODULES["dbt_run_cache"]
        assert signal_run_cache.env_var == "DBT_ENGINE_STATE_DISABLED"
        assert signal_run_cache.flag_attr == "STATE_PLUGIN_DISABLED"

    def test_opt_out_registry_is_read_only(self):
        # Wrapped in MappingProxyType so a buggy caller can't silently flip behavior
        # for the rest of the process.
        with pytest.raises(TypeError):
            PluginManager.OPT_OUT_PLUGIN_MODULES["dbt_state"] = None  # type: ignore[index]
        with pytest.raises((TypeError, AttributeError)):
            PluginManager.OPT_OUT_PLUGIN_MODULES.pop("dbt_state")  # type: ignore[attr-defined]

    def test_disabled_opt_out_modules_default_empty(self, monkeypatch):
        """With no env var and no project flag, nothing is disabled -- bundled plugins
        load by default."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)
        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False):
            disabled = PluginManager._disabled_opt_out_modules()
        assert disabled == set()

    def test_disabled_opt_out_modules_when_env_var_set(self, monkeypatch):
        monkeypatch.setenv("DBT_ENGINE_STATE_DISABLED", "true")
        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False):
            disabled = PluginManager._disabled_opt_out_modules()
        assert "dbt_state" in disabled
        assert "dbt_run_cache" in disabled

    def test_disabled_opt_out_modules_when_project_flag_set(self, monkeypatch):
        """The project flag is the second opt-out path -- set via `flags:` in
        dbt_project.yml or `config:` in profiles.yml. Plugin must be skipped exactly
        as if the env var were set."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)
        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=True):
            disabled = PluginManager._disabled_opt_out_modules()
        assert "dbt_state" in disabled
        assert "dbt_run_cache" in disabled

    # NOTE on patch ordering in the tests below: `mock.patch` resolves its target via
    # `importlib.import_module`. If we patch `dbt.plugins.manager.importlib.import_module`
    # FIRST, any subsequent patch on `dbt.plugins.manager.<attr>` in the same with-block
    # silently fails -- mock.patch's own resolver hits the mocked import_module and gets a
    # MagicMock back instead of the real module. So `logger` / `_notify_*` /
    # `_read_project_flag` patches go first; `importlib.import_module` last.

    def test_get_prefixed_modules_loads_by_default(self, monkeypatch):
        """No opt-out signal -> bundled plugin imports normally."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)
        fake_iter_result = [
            (None, "dbt_state", False),
            (None, "dbt_someother", False),
            (None, "unrelated_pkg", False),
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" in modules
        assert "dbt_someother" in modules
        assert "unrelated_pkg" not in modules
        assert "dbt_state" in imported

    def test_get_prefixed_modules_skips_when_env_var_set(self, monkeypatch):
        """Env-var opt-out: bundled plugin must be filtered out BEFORE
        `importlib.import_module` is called."""
        monkeypatch.setenv("DBT_ENGINE_STATE_DISABLED", "true")
        fake_iter_result = [
            (None, "dbt_state", False),
            (None, "dbt_someother", False),
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" not in modules
        assert "dbt_someother" in modules
        # Zero-cost: the disabled plugin is never imported.
        assert "dbt_state" not in imported

    def test_get_prefixed_modules_skips_when_project_flag_set(self, monkeypatch):
        """Project-flag opt-out: same end result as the env-var path."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)
        fake_iter_result = [(None, "dbt_state", False)]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=True), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" not in modules
        assert "dbt_state" not in imported

    def test_discovery_logs_info_when_env_var_disables(self, monkeypatch):
        """Skipping should emit a one-time INFO naming the source (env var)."""
        monkeypatch.setenv("DBT_ENGINE_STATE_DISABLED", "true")
        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False), mock.patch(
            "dbt.plugins.manager.logger"
        ) as mock_logger, mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch(
            "dbt.plugins.manager.importlib.import_module"
        ):
            PluginManager.get_prefixed_modules()

        mock_logger.info.assert_called_once()
        args = mock_logger.info.call_args[0]
        # Message should mention the module and the source ("env var DBT_ENGINE_STATE_DISABLED").
        assert "dbt_state" in args
        assert any("DBT_ENGINE_STATE_DISABLED" in str(a) for a in args)

    def test_discovery_logs_info_when_project_flag_disables(self, monkeypatch):
        """Skipping should also emit a notice when the project flag is the source."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)
        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=True), mock.patch(
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
        assert any("state_plugin_disabled" in str(a).lower() for a in args)

    def test_discovery_notice_is_emitted_only_once_per_process(self, monkeypatch):
        monkeypatch.setenv("DBT_ENGINE_STATE_DISABLED", "true")
        fake_iter_result = [(None, "dbt_state", False)]

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False), mock.patch(
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

    def test_conflict_resolution_prefers_dbt_state_over_dbt_run_cache(self, monkeypatch):
        """If both packages happen to be installed (mid-rename), we must NOT load both
        -- double monkey-patching of CompileRunner/ModelRunner is non-deterministic.
        Prefer the canonical name and warn about the loser."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)
        fake_iter_result = [
            (None, "dbt_state", False),
            (None, "dbt_run_cache", False),
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False), mock.patch(
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

    def test_from_modules_instantiates_by_default(self, monkeypatch):
        """End-to-end contract: with no opt-out signal, the bundled plugin is
        instantiated."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)

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

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            PluginManager.from_modules(project_name="test")

        assert instantiated == ["dbt_state"]

    def test_from_modules_does_not_instantiate_when_env_var_disables(self, monkeypatch):
        """End-to-end contract: env-var opt-out prevents plugin instantiation."""
        monkeypatch.setenv("DBT_ENGINE_STATE_DISABLED", "true")

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

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=False), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            pm = PluginManager.from_modules(project_name="test")

        assert instantiated == [], "env-var-disabled plugin must not be instantiated"
        assert pm.hooks == {}

    def test_from_modules_does_not_instantiate_when_project_flag_disables(self, monkeypatch):
        """End-to-end contract: project-flag opt-out also prevents instantiation."""
        monkeypatch.delenv("DBT_ENGINE_STATE_DISABLED", raising=False)

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

        with mock.patch("dbt.plugins.manager._read_project_flag", return_value=True), mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            pm = PluginManager.from_modules(project_name="test")

        assert instantiated == [], "project-flag-disabled plugin must not be instantiated"
        assert pm.hooks == {}


class TestProjectFlagsExposeStatePluginDisabled:
    """The state_plugin_disabled flag must be wired through ProjectFlags so that
    dbt_project.yml's `flags:` block and profiles.yml's `config:` block both work
    (they share the same parser via `read_project_flags`)."""

    def test_default_is_false(self):
        from dbt.contracts.project import ProjectFlags

        assert ProjectFlags().state_plugin_disabled is False

    def test_can_be_set_from_dict(self):
        from dbt.contracts.project import ProjectFlags

        pf = ProjectFlags.from_dict({"state_plugin_disabled": True})
        assert pf.state_plugin_disabled is True

    def test_is_in_project_only_flags(self):
        """project_only_flags is the dict that cli/flags.py iterates to setattr
        UPPERCASE attrs on the global Flags object. If this key is missing,
        `get_flags().STATE_PLUGIN_DISABLED` is never populated and the project-flag
        opt-out path silently does nothing."""
        from dbt.contracts.project import ProjectFlags

        pf = ProjectFlags(state_plugin_disabled=True)
        assert pf.project_only_flags["state_plugin_disabled"] is True
