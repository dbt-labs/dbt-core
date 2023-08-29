import pytest
from dbt.exceptions import ParsingError
from dbt.contracts.graph.model_config import SemanticModelConfig

from dbt.tests.util import run_dbt, update_config_file, get_manifest

from tests.functional.semantic_models.fixtures import (
    models_people_sql,
    metricflow_time_spine_sql,
    semantic_model_people_yml,
    disabled_models_people_metrics_yml,
    models_people_metrics_yml,
    disabled_semantic_model_people_yml,
    enabled_semantic_model_people_yml,
)


# Test disabled config at semantic_models level in yaml file
class TestConfigYamlLevel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_models.yml": disabled_semantic_model_people_yml,
            "people_metrics.yml": disabled_models_people_metrics_yml,
        }

    def test_yaml_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "semantic_model.test.semantic_people" not in manifest.semantic_models
        assert "semantic_model.test.semantic_people" in manifest.disabled


# Test disabled config at semantic_models level with a still enabled metric
class TestDisabledConfigYamlLevelEnabledMetric:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_models.yml": disabled_semantic_model_people_yml,
            "people_metrics.yml": models_people_metrics_yml,
        }

    def test_yaml_level(self, project):
        with pytest.raises(
            ParsingError,
            match="A semantic model having a measure `people` is disabled but was referenced",
        ):
            run_dbt(["parse"])


# Test disabling semantic model config but not metric config in dbt_project.yml
class TestMismatchesConfigProjectLevel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_models.yml": semantic_model_people_yml,
            "people_metrics.yml": models_people_metrics_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "semantic-models": {
                "test": {
                    "enabled": True,
                }
            }
        }

    def test_project_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "semantic_model.test.semantic_people" in manifest.semantic_models

        new_enabled_config = {
            "semantic-models": {
                "test": {
                    "enabled": False,
                }
            }
        }
        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        with pytest.raises(
            ParsingError,
            match="A semantic model having a measure `people` is disabled but was referenced",
        ):
            run_dbt(["parse"])


# Test disabling semantic model and metric configs in dbt_project.yml
class TestConfigProjectLevel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_models.yml": semantic_model_people_yml,
            "people_metrics.yml": models_people_metrics_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "semantic-models": {
                "test": {
                    "enabled": True,
                }
            },
            "metrics": {
                "test": {
                    "enabled": True,
                }
            },
        }

    def test_project_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "semantic_model.test.semantic_people" in manifest.semantic_models

        new_enabled_config = {
            "semantic-models": {
                "test": {
                    "enabled": False,
                }
            },
            "metrics": {
                "test": {
                    "enabled": False,
                }
            },
        }
        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        assert "semantic_model.test.semantic_people" not in manifest.semantic_models
        assert "semantic_model.test.semantic_people" in manifest.disabled


# Test inheritence - set configs at project and semantic_model level - expect semantic_model level to win
class TestConfigsInheritence:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_models.yml": enabled_semantic_model_people_yml,
            "people_metrics.yml": models_people_metrics_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"semantic-models": {"enabled": False}}

    def test_project_plus_yaml_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "semantic_model.test.semantic_people" in manifest.semantic_models
        config_test_table = manifest.semantic_models.get(
            "semantic_model.test.semantic_people"
        ).config

        assert isinstance(config_test_table, SemanticModelConfig)
