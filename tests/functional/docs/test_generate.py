import pytest
from unittest import mock

from dbt.plugins.manifest import PluginNodes, ModelNodeArgs
from dbt.tests.util import run_dbt, get_manifest
import json


class TestGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun"}

    def test_manifest_not_compiled(self, project):
        run_dbt(["docs", "generate", "--no-compile"])
        # manifest.json is written out in parsing now, but it
        # shouldn't be compiled because of the --no-compile flag
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        assert model_id in manifest.nodes
        assert manifest.nodes[model_id].compiled is False

    def test_generate_empty_catalog(self, project):
        run_dbt(["docs", "generate", "--empty-catalog"])
        with open("./target/catalog.json") as file:
            catalog = json.load(file)
        assert catalog["nodes"] == {}, "nodes should be empty"
        assert catalog["sources"] == {}, "sources should be empty"
        assert catalog["errors"] is None, "errors should be null"


class TestGenerateCatalogWithExternalNodes(TestGenerate):
    @mock.patch("dbt.plugins.get_plugin_manager")
    def test_catalog_with_sources(self, get_plugin_manager, project):
        project.run_sql("create table {}.external_model (id int)".format(project.test_schema))

        run_dbt(["build"])

        external_nodes = PluginNodes()
        external_model_node = ModelNodeArgs(
            name="external_model",
            package_name="external_package",
            identifier="external_model",
            schema=project.test_schema,
            database="dbt",
        )
        external_nodes.add_model(external_model_node)
        get_plugin_manager.return_value.get_nodes.return_value = external_nodes
        catalog = run_dbt(["docs", "generate"])

        assert "model.external_package.external_model" in catalog.nodes
