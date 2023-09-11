import pytest
from dbt.dataclass_schema import ValidationError

from dbt.contracts.graph.model_config import ExposureConfig

from dbt.tests.util import run_dbt, update_config_file, get_manifest
from tests.functional.exposures.fixtures import (
    models_sql,
    second_model_sql,
    simple_exposure_yml,
    disabled_models_exposure_yml,
    enabled_yaml_level_exposure_yml,
    invalid_config_exposure_yml,
    source_schema_yml,
    metricflow_time_spine_sql,
    semantic_models_schema_yml,
    metrics_schema_yml,
)


class ExposureConfigTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        pytest.expected_config = ExposureConfig(
            enabled=True,
        )


# Test enabled config for exposure in dbt_project.yml
class TestExposureEnabledConfigProjectLevel(ExposureConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "second_model.sql": second_model_sql,
            "exposure.yml": simple_exposure_yml,
            "schema.yml": source_schema_yml,
            "semantic_models.yml": semantic_models_schema_yml,
            "metrics.yml": metrics_schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "exposures": {
                "simple_exposure": {
                    "enabled": True,
                },
            }
        }

    def test_enabled_exposure_config_dbt_project(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "exposure.test.simple_exposure" in manifest.exposures

        new_enabled_config = {
            "exposures": {
                "test": {
                    "simple_exposure": {
                        "enabled": False,
                    },
                }
            }
        }
        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "exposure.test.simple_exposure" not in manifest.exposures
        assert "exposure.test.notebook_exposure" in manifest.exposures


# Test disabled config at exposure level in yml file
class TestConfigYamlLevel(ExposureConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": disabled_models_exposure_yml,
        }

    def test_exposure_config_yaml_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "exposure.test.simple_exposure" not in manifest.exposures
        assert "exposure.test.notebook_exposure" in manifest.exposures


# Test inheritence - set configs at project and exposure level - expect exposure level to win
class TestExposureConfigsInheritence(ExposureConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": enabled_yaml_level_exposure_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"exposures": {"enabled": False}}

    def test_exposure_all_configs(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        # This should be overridden
        assert "exposure.test.simple_exposure" in manifest.exposures
        # This should stay disabled
        assert "exposure.test.notebook_exposure" not in manifest.exposures

        config_test_table = manifest.exposures.get("exposure.test.simple_exposure").config

        assert isinstance(config_test_table, ExposureConfig)
        assert config_test_table == pytest.expected_config


# Test invalid config triggers error
class TestInvalidConfig(ExposureConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": invalid_config_exposure_yml,
        }

    def test_exposure_config_yaml_level(self, project):
        with pytest.raises(ValidationError) as excinfo:
            run_dbt(["parse"])
        expected_msg = "'True and False' is not of type 'boolean'"
        assert expected_msg in str(excinfo.value)
