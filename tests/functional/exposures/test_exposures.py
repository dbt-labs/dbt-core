import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import Exposure
from dbt.tests.util import get_manifest, run_dbt
from tests.functional.exposures.fixtures import (
    metricflow_time_spine_sql,
    metrics_schema_yml,
    models_sql,
    second_model_sql,
    semantic_models_schema_yml,
    simple_exposure_yml,
    source_schema_yml,
)


class TestBasicExposures:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "exposure.yml": simple_exposure_yml,
            "model.sql": models_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": source_schema_yml,
            "semantic_models.yml": semantic_models_schema_yml,
            "metrics.yml": metrics_schema_yml,
        }

    def test_compilation_names_with_spaces(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        exposure_ids = list(manifest.exposures.keys())
        expected_exposure_ids = [
            "exposure.test.simple_exposure",
            "exposure.test.notebook_exposure",
        ]
        assert exposure_ids == expected_exposure_ids
        assert manifest.exposures["exposure.test.simple_exposure"].label == "simple exposure label"

    def test_compilation_depends_on(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        exposure_depends_on = manifest.exposures["exposure.test.simple_exposure"].depends_on.nodes
        expected_exposure_depends_on = [
            "source.test.test_source.test_table",
            "model.test.model",
            "metric.test.metric",
        ]
        assert sorted(exposure_depends_on) == sorted(expected_exposure_depends_on)

    def test_execution_default(self, project):
        results = run_dbt(["build"])
        exposure_results = (
            result for result in results.results if isinstance(result.node, Exposure)
        )
        assert {result.node.name for result in exposure_results} == {
            "simple_exposure",
            "notebook_exposure",
        }
        assert all(result.status == RunStatus.NoOp for result in exposure_results)
        assert all("NO-OP" in result.message for result in exposure_results)

    def test_execution_exclude(self, project):
        results = run_dbt(["build", "--exclude", "simple_exposure"])
        exposure_results = (
            result for result in results.results if isinstance(result.node, Exposure)
        )
        assert {result.node.name for result in exposure_results} == {"notebook_exposure"}

    def test_execution_select(self, project):
        results = run_dbt(["build", "--select", "simple_exposure"])
        exposure_results = (
            result for result in results.results if isinstance(result.node, Exposure)
        )
        assert {result.node.name for result in exposure_results} == {"simple_exposure"}
