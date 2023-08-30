import pytest
from dbt.dataclass_schema import ValidationError

from dbt.contracts.graph.model_config import SourceConfig

from dbt.tests.util import run_dbt, update_config_file, get_manifest
from tests.functional.sources.fixtures import (
    basic_source_schema_yml,
    disabled_source_level_schema_yml,
    disabled_source_table_schema_yml,
    all_configs_everywhere_schema_yml,
    all_configs_not_table_schema_yml,
    all_configs_project_source_schema_yml,
    invalid_config_source_schema_yml,
)


class SourceConfigTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        pytest.expected_config = SourceConfig(
            enabled=True,
        )


# Test enabled config in dbt_project.yml
# expect pass, already implemented
class TestSourceEnabledConfigProjectLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": basic_source_schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "sources": {
                "test": {
                    "test_source": {
                        "enabled": True,
                    },
                }
            }
        }

    def test_enabled_source_config_dbt_project(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources

        new_enabled_config = {
            "sources": {
                "test": {
                    "test_source": {
                        "enabled": False,
                    },
                }
            }
        }
        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        assert (
            "source.test.test_source.test_table" not in manifest.sources
        )  # or should it be there with enabled: false??
        assert "source.test.other_source.test_table" in manifest.sources


# Test enabled config at sources level in yml file
class TestConfigYamlSourceLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": disabled_source_level_schema_yml,
        }

    def test_source_config_yaml_source_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" not in manifest.sources
        assert "source.test.test_source.disabled_test_table" not in manifest.sources


# Test enabled config at source table level in yaml file
class TestConfigYamlSourceTable(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": disabled_source_table_schema_yml,
        }

    def test_source_config_yaml_source_table(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.disabled_test_table" not in manifest.sources


# Test inheritence - set configs at project, source, and source-table level - expect source-table level to win
class TestSourceConfigsInheritence1(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_everywhere_schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"sources": {"enabled": True}}

    def test_source_all_configs_source_table(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" not in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config

        assert isinstance(config_test_table, SourceConfig)
        assert config_test_table == pytest.expected_config


# Test inheritence - set configs at project and source level - expect source level to win
class TestSourceConfigsInheritence2(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_not_table_schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"sources": {"enabled": False}}

    def test_source_two_configs_source_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config
        config_other_test_table = manifest.sources.get(
            "source.test.test_source.other_test_table"
        ).config

        assert isinstance(config_test_table, SourceConfig)
        assert isinstance(config_other_test_table, SourceConfig)

        assert config_test_table == config_other_test_table
        assert config_test_table == pytest.expected_config


# Test inheritence - set configs at project and source-table level - expect source-table level to win
class TestSourceConfigsInheritence3(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_project_source_schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"sources": {"enabled": False}}

    def test_source_two_configs_source_table(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.other_test_table" not in manifest.sources
        config_test_table = manifest.sources.get("source.test.test_source.test_table").config

        assert isinstance(config_test_table, SourceConfig)
        assert config_test_table == pytest.expected_config


# Test invalid source configs
class TestInvalidSourceConfig(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": invalid_config_source_schema_yml,
        }

    def test_invalid_config_source(self, project):
        with pytest.raises(ValidationError) as excinfo:
            run_dbt(["parse"])
        expected_msg = "'True and False' is not of type 'boolean'"
        assert expected_msg in str(excinfo.value)
