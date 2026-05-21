import json
import os

import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.semantic_manifest import SemanticManifest
from dbt.tests.util import write_file
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.semantic_models.fixtures import (
    base_schema_yml_v2,
    fct_revenue_sql,
    simple_metricflow_time_spine_sql,
)


def _osi_doc_json(sm_name: str, source: str) -> str:
    return json.dumps(
        {
            "version": "0.1.1",
            "semantic_model": [
                {"name": sm_name, "datasets": [{"name": sm_name, "source": source}]}
            ],
        }
    )


def _write_osi_file(project, sm_name: str, filename: str) -> None:
    source = f"{project.database}.{project.test_schema}.fct_revenue"
    osi_dir = os.path.join(project.project_root, "OSI")
    os.makedirs(osi_dir, exist_ok=True)
    write_file(_osi_doc_json(sm_name, source), osi_dir, filename)


class TestOsiDocumentParsedIntoManifest:
    """OSI documents in OSI/ are parsed into the manifest and semantic_manifest."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": simple_metricflow_time_spine_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_osi_directory(self, project):
        _write_osi_file(project, "osi_orders", "orders.json")

    def test_osi_semantic_model_appears_in_manifest(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success, result.exception
        assert isinstance(result.result, Manifest)
        manifest = result.result

        uid = "semantic_model.test.osi_orders"
        assert uid in manifest.semantic_models
        sm = manifest.semantic_models[uid]
        assert sm.name == "osi_orders"
        assert sm.package_name == "test"
        assert sm.path == os.path.join("OSI", "orders.json")
        from dbt.artifacts.resources import RefArgs

        assert sm.refs == [RefArgs(name="fct_revenue", package=None, version=None)]

    def test_osi_semantic_model_appears_in_semantic_manifest(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success, result.exception
        manifest = result.result

        pydantic_sm = SemanticManifest(manifest)._get_pydantic_semantic_manifest()
        sm_names = {sm.name for sm in pydantic_sm.semantic_models}
        assert "osi_orders" in sm_names


class TestOsiAndNativeDbtSemanticModelsCoexist:
    """An OSI-sourced semantic model and a native dbt semantic model can coexist."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml_v2,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": simple_metricflow_time_spine_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_osi_directory(self, project):
        # OSI defines a second semantic model on the same table under a different name
        _write_osi_file(project, "osi_fct_revenue", "fct_revenue.json")

    def test_both_semantic_models_present(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success, result.exception
        manifest = result.result

        # Native dbt v2-YAML semantic model
        assert "semantic_model.test.fct_revenue" in manifest.semantic_models
        # OSI-sourced semantic model
        assert "semantic_model.test.osi_fct_revenue" in manifest.semantic_models
        assert len(manifest.semantic_models) == 2

    def test_both_appear_in_semantic_manifest(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success, result.exception
        manifest = result.result

        pydantic_sm = SemanticManifest(manifest)._get_pydantic_semantic_manifest()
        sm_names = {sm.name for sm in pydantic_sm.semantic_models}
        assert "fct_revenue" in sm_names
        assert "osi_fct_revenue" in sm_names
