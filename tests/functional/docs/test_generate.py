import pytest
from unittest import mock

from dbt.plugins.manifest import PluginNodes, ModelNodeArgs
from dbt.tests.util import run_dbt, get_manifest

sample_seed = """sample_num,sample_bool
1,true
2,false
3,true
"""

second_seed = """sample_num,sample_bool
4,true
5,false
6,true
"""

sample_config = """
sources:
  - name: my_seed
    schema: "{{ target.schema }}"
    tables:
      - name: sample_seed
      - name: second_seed
      - name: fake_seed
"""


class TestBaseGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as fun",
            "alt_model.sql": "select 1 as notfun",
            "sample_config.yml": sample_config,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed,
            "second_seed.csv": sample_seed,
        }


class TestGenerateManifestNotCompiled(TestBaseGenerate):
    def test_manifest_not_compiled(self, project):
        run_dbt(["docs", "generate", "--no-compile"])
        # manifest.json is written out in parsing now, but it
        # shouldn't be compiled because of the --no-compile flag
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        assert model_id in manifest.nodes
        assert manifest.nodes[model_id].compiled is False


class TestGenerateEmptyCatalog(TestBaseGenerate):
    def test_generate_empty_catalog(self, project):
        catalog = run_dbt(["docs", "generate", "--empty-catalog"])
        assert catalog.nodes == {}, "nodes should be empty"
        assert catalog.sources == {}, "sources should be empty"
        assert catalog.errors is None, "errors should be null"


class TestGenerateSelectLimitsCatalog(TestBaseGenerate):
    def test_select_limits_catalog(self, project):
        run_dbt(["run"])
        catalog = run_dbt(["docs", "generate", "--select", "my_model"])
        assert len(catalog.nodes) == 1
        assert "model.test.my_model" in catalog.nodes


class TestGenerateSelectLimitsNoMatch(TestBaseGenerate):
    def test_select_limits_no_match(self, project):
        run_dbt(["run"])
        catalog = run_dbt(["docs", "generate", "--select", "my_missing_model"])
        assert len(catalog.nodes) == 0


class TestGenerateCatalogWithSources(TestBaseGenerate):
    def test_catalog_with_sources(self, project):
        run_dbt(["build"])
        catalog = run_dbt(["docs", "generate"])

        # 2 seeds + 2 models
        assert len(catalog.nodes) == 4
        # 2 sources (only ones that exist)
        assert len(catalog.sources) == 2


class TestGenerateCatalogWithExternalNodes(TestBaseGenerate):
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


class TestGenerateSelectSource(TestBaseGenerate):
    def test_select_source(self, project):
        run_dbt(["build"])
        catalog = run_dbt(["docs", "generate", "--select", "source:test.my_seed.sample_seed"])

        # 2 seeds
        # TODO: Filtering doesn't work for seeds
        assert len(catalog.nodes) == 2
        # 2 sources
        # TODO: Filtering doesn't work for sources
        assert len(catalog.sources) == 2
