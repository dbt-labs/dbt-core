import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import CompilationError
from dbt.tests.util import run_dbt
from tests.functional.semantic_models.fixtures import (
    models_people_sql,
    simple_metricflow_time_spine_sql,
    semantic_model_people_yml,
    models_people_metrics_yml,
    semantic_model_people_yml_with_docs,
    semantic_model_descriptions,
)


class TestSemanticModelDependsOn:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": simple_metricflow_time_spine_sql,
            "semantic_models.yml": semantic_model_people_yml,
            "people_metrics.yml": models_people_metrics_yml,
        }

    def test_depends_on(self, project):
        manifest = run_dbt(["parse"])
        assert isinstance(manifest, Manifest)

        expected_depends_on_for_people_semantic_model = ["model.test.people"]

        number_of_people_metric = manifest.semantic_models["semantic_model.test.semantic_people"]
        assert (
            number_of_people_metric.depends_on.nodes
            == expected_depends_on_for_people_semantic_model
        )


class TestSemanticModelNestedDocs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": simple_metricflow_time_spine_sql,
            "semantic_models.yml": semantic_model_people_yml_with_docs,
            "people_metrics.yml": models_people_metrics_yml,
            "docs.md": semantic_model_descriptions,
        }

    def test_depends_on(self, project):
        manifest = run_dbt(["parse"])
        node = manifest.semantic_models["semantic_model.test.semantic_people"]

        assert node.description == "foo"
        assert node.dimensions[0].description == "bar"
        assert node.measures[0].description == "baz"
        assert node.entities[0].description == "qux"


class TestSemanticModelUnknownModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "not_people.sql": models_people_sql,
            "metricflow_time_spine.sql": simple_metricflow_time_spine_sql,
            "semantic_models.yml": semantic_model_people_yml,
            "people_metrics.yml": models_people_metrics_yml,
        }

    def test_unknown_model_raises_issue(self, project):
        with pytest.raises(CompilationError) as excinfo:
            run_dbt(["parse"])
        assert "depends on a node named 'people' which was not found" in str(excinfo.value)
