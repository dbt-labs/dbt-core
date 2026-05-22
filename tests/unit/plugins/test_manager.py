from unittest import mock

import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.plugins import PluginManager, dbt_hook, dbtPlugin
from dbt.plugins.contracts import PluginArtifact, PluginArtifacts
from dbt.plugins.exceptions import dbtPluginError
from dbt.plugins.manager import _env_gate_is_on
from dbt.plugins.manifest import ModelNodeArgs, PluginNodes


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


class TestOptInPluginGating:
    """Bundled-but-opt-in plugins (e.g. dbt-state) must be skipped at discovery time
    unless their gate env var is tripped. The skip happens BEFORE importlib.import_module,
    so a disabled opt-in plugin pays zero import cost."""

    def test_env_gate_unset(self, monkeypatch):
        monkeypatch.delenv("DBT_TEST_GATE", raising=False)
        assert _env_gate_is_on("DBT_TEST_GATE") is False

    @pytest.mark.parametrize(
        "value", ["true", "True", "TRUE", "1", "t", "y", "yes", "on", " on ", "ON"]
    )
    def test_env_gate_truthy_values(self, monkeypatch, value):
        monkeypatch.setenv("DBT_TEST_GATE", value)
        assert _env_gate_is_on("DBT_TEST_GATE") is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "disabled", "anything"])
    def test_env_gate_non_truthy_values(self, monkeypatch, value):
        monkeypatch.setenv("DBT_TEST_GATE", value)
        assert _env_gate_is_on("DBT_TEST_GATE") is False

    def test_opt_in_registry_contains_both_module_names(self):
        # dbt-state is bundled with dbt-core but must be gated by default. Both
        # `dbt_state` (new package name) and `dbt_run_cache` (legacy package name from
        # before the run-cache -> dbt-state rename) refer to the same plugin; whichever
        # module name pkgutil discovers in the user's environment must be skipped
        # unless the gate is on. If either mapping is removed, the plugin will
        # auto-load on every dbt invocation -- a behavior change requiring explicit
        # sign-off. See pyproject.toml.
        assert PluginManager.OPT_IN_PLUGIN_MODULES["dbt_state"] == "DBT_STATE_ENABLED"
        assert PluginManager.OPT_IN_PLUGIN_MODULES["dbt_run_cache"] == "DBT_STATE_ENABLED"

    def test_disabled_opt_in_modules_default(self, monkeypatch):
        monkeypatch.delenv("DBT_STATE_ENABLED", raising=False)
        disabled = PluginManager._disabled_opt_in_modules()
        assert "dbt_state" in disabled
        assert "dbt_run_cache" in disabled

    def test_disabled_opt_in_modules_when_gate_on(self, monkeypatch):
        monkeypatch.setenv("DBT_STATE_ENABLED", "true")
        disabled = PluginManager._disabled_opt_in_modules()
        assert "dbt_state" not in disabled
        assert "dbt_run_cache" not in disabled

    def test_get_prefixed_modules_skips_disabled_opt_in(self, monkeypatch):
        """Discovery must filter opt-in modules out of the iter_modules walk so
        importlib.import_module is never called on them when the gate is off."""
        monkeypatch.delenv("DBT_STATE_ENABLED", raising=False)

        fake_iter_result = [
            (None, "dbt_state", False),  # opt-in, gate off -> must be skipped
            (None, "dbt_someother", False),  # regular plugin -> imported
            (None, "unrelated_pkg", False),  # wrong prefix -> skipped
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" not in modules
        assert "dbt_someother" in modules
        assert "unrelated_pkg" not in modules
        # Critical: importlib.import_module was never called for the gated-off plugin.
        assert "dbt_state" not in imported

    def test_get_prefixed_modules_loads_opt_in_when_gate_on(self, monkeypatch):
        monkeypatch.setenv("DBT_STATE_ENABLED", "1")

        fake_iter_result = [
            (None, "dbt_state", False),
            (None, "dbt_someother", False),
        ]
        imported = []

        def fake_import(name):
            imported.append(name)
            return mock.MagicMock()

        with mock.patch(
            "dbt.plugins.manager.pkgutil.iter_modules", return_value=fake_iter_result
        ), mock.patch("dbt.plugins.manager.importlib.import_module", side_effect=fake_import):
            modules = PluginManager.get_prefixed_modules()

        assert "dbt_state" in modules
        assert "dbt_state" in imported
