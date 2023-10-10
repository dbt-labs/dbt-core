import pytest

from dbt.tests.util import run_dbt, get_manifest


class TestGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun", "alt_model.sql": "select 1 as notfun"}

    def test_manifest_not_compiled(self, project):
        run_dbt(["docs", "generate", "--no-compile"])
        # manifest.json is written out in parsing now, but it
        # shouldn't be compiled because of the --no-compile flag
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        assert model_id in manifest.nodes
        assert manifest.nodes[model_id].compiled is False

    def test_generate_empty_catalog(self, project):
        catalog = run_dbt(["docs", "generate", "--empty-catalog"])
        assert catalog.nodes == {}, "nodes should be empty"
        assert catalog.sources == {}, "sources should be empty"
        assert catalog.errors is None, "errors should be null"

    def test_select_limits_catalog(self, project):
        run_dbt(["run"])
        catalog = run_dbt(["docs", "generate", "--select", "my_model"])
        assert len(catalog.nodes) == 1
        assert "model.test.my_model" in catalog.nodes
