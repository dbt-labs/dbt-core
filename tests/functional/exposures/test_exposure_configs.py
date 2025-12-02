import pytest

from dbt.artifacts.resources import ExposureConfig
from dbt.tests.util import get_manifest, run_dbt, update_config_file
from dbt_common.dataclass_schema import ValidationError
from tests.functional.exposures.fixtures import (
    disabled_models_exposure_yml,
    enabled_yaml_level_exposure_yml,
    invalid_config_exposure_yml,
    metricflow_time_spine_sql,
    metrics_schema_yml,
    models_sql,
    second_model_sql,
    semantic_models_schema_yml,
    simple_exposure_yml,
    source_schema_yml,
)


# Test enabled config for exposure in dbt_project.yml
class TestExposureEnabledConfigProjectLevel:
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
class TestConfigYamlLevel:
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
class TestExposureConfigsInheritence:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": enabled_yaml_level_exposure_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "exposures": {
                "enabled": False,
                "tags": ["global_tag", "common_tag"],
                "meta": {
                    "some_key": "should_be_overridden",
                    "another_key": "should_stay",
                    "type_change": ["foo", "bar"],
                },
            }
        }

    @pytest.fixture(scope="class")
    def expected_config(self):
        return ExposureConfig(
            enabled=True,
            tags=["common_tag", "global_tag", "local_tag"],
            meta={"some_key": "some_value", "another_key": "should_stay", "type_change": 123},
        )

    def test_exposure_all_configs(self, project, expected_config):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        # This should be overridden
        assert "exposure.test.simple_exposure" in manifest.exposures
        # This should stay disabled
        assert "exposure.test.notebook_exposure" not in manifest.exposures

        exposure = manifest.exposures.get("exposure.test.simple_exposure")
        assert exposure.tags == expected_config.tags
        assert exposure.meta == expected_config.meta

        assert isinstance(exposure.config, ExposureConfig)
        assert exposure.config.enabled == expected_config.enabled
        assert exposure.config.tags == expected_config.tags
        assert exposure.config.meta == expected_config.meta


# Test invalid config triggers error
class TestInvalidConfig:
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
