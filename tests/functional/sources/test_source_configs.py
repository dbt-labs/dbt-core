import pytest
from dbt.contracts.graph.model_config import SourceConfig

from dbt.tests.util import run_dbt, update_config_file, get_manifest


class SourceConfigTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        pytest.expected_config = SourceConfig(
            enabled=True,
        )


models__schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
  - name: other_source
    tables:
      - name: test_table
"""


# Test enabled config in dbt_project.yml
# expect pass, already implemented
class TestSourceEnabledConfigProjectLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
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


disabled_source_level__schema_yml = """version: 2

sources:
  - name: test_source
    config:
      enabled: False
    tables:
      - name: test_table
      - name: disabled_test_table
"""


# Test enabled config at sources level in yml file
class TestConfigYamlSourceLevel(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": disabled_source_level__schema_yml,
        }

    def test_source_config_yaml_source_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" not in manifest.sources
        assert "source.test.test_source.disabled_test_table" not in manifest.sources


disabled_source_table__schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
      - name: disabled_test_table
        config:
            enabled: False
"""


# Test enabled config at source table level in yaml file
class TestConfigYamlSourceTable(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": disabled_source_table__schema_yml,
        }

    def test_source_config_yaml_source_table(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.test_table" in manifest.sources
        assert "source.test.test_source.disabled_test_table" not in manifest.sources


all_configs_everywhere__schema_yml = """version: 2

sources:
  - name: test_source
    config:
        enabled: False
    tables:
      - name: test_table
        config:
            enabled: True
      - name: other_test_table
"""


# Test inheritence - set configs at project, source, and source-table level - expect source-table level to win
class TestSourceConfigsInheritence1(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_everywhere__schema_yml}

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


all_configs_not_table_schema_yml = """version: 2

sources:
  - name: test_source
    config:
        enabled: True
    tables:
      - name: test_table
      - name: other_test_table
"""


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


all_configs_project_source__schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
        config:
            enabled: True
      - name: other_test_table
"""


# Test inheritence - set configs at project and source-table level - expect source-table level to win
class TestSourceConfigsInheritence3(SourceConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": all_configs_project_source__schema_yml}

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
