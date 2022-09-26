import pytest
from dbt.tests.util import run_dbt, get_manifest

from dbt.exceptions import CompilationException, ParsingException

from tests.functional.configs.fixtures import (
    schema_all_disabled_yml,
    schema_partial_enabled_yml,
    schema_partial_disabled_yml,
    schema_explicit_enabled_yml,
    my_model,
    my_model_2,
    my_model_2_enabled,
    my_model_2_disabled,
    my_model_3,
    my_model_3_disabled,
)


# ensure double disabled doesn't throw error when set at schema level
class TestSchemaDisabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_all_disabled_yml,
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2,
            "my_model_3.sql": my_model_3,
        }

    def test_disabled_config(self, project):
        run_dbt(["parse"])


# ensure this throws a specific error that the model is disabled
class TestSchemaDisabledConfigsFailure:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_partial_disabled_yml,
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2,
            "my_model_3.sql": my_model_3,
        }

    def test_disabled_config(self, project):
        with pytest.raises(CompilationException) as exc:
            run_dbt(["parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "which is disabled"
        assert expected_msg in exc_str


# ensure double disabled doesn't throw error when set in model configs
class TestModelDisabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_disabled,
            "my_model_3.sql": my_model_3_disabled,
        }

    def test_disabled_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" not in manifest.nodes
        assert "model.test.my_model_3" not in manifest.nodes

        assert "model.test.my_model_2" in manifest.disabled
        assert "model.test.my_model_3" in manifest.disabled


# ensure config set in project.yml can be overridden in yaml file
class TestOverrideProjectConfigsInYaml:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_partial_enabled_yml,
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2,
            "my_model_3.sql": my_model_3,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "my_model_2": {
                        "enabled": False,
                    },
                    "my_model_3": {
                        "enabled": False,
                    },
                },
            }
        }

    def test_override_project_yaml_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" in manifest.nodes
        assert "model.test.my_model_3" not in manifest.nodes

        assert "model.test.my_model_2" not in manifest.disabled
        assert "model.test.my_model_3" in manifest.disabled


# ensure config set in project.yml can be overridden in sql file
class TestOverrideProjectConfigsInSQL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_enabled,
            "my_model_3.sql": my_model_3,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "my_model_2": {
                        "enabled": False,
                    },
                    "my_model_3": {
                        "enabled": False,
                    },
                },
            }
        }

    def test_override_project_sql_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" in manifest.nodes
        assert "model.test.my_model_3" not in manifest.nodes

        assert "model.test.my_model_2" not in manifest.disabled
        assert "model.test.my_model_3" in manifest.disabled


# ensure false config set in yaml file can be overridden in sql file
class TestOverrideFalseYAMLConfigsInSQL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_all_disabled_yml,
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_enabled,
            "my_model_3.sql": my_model_3,
        }

    def test_override_yaml_sql_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" in manifest.nodes
        assert "model.test.my_model_3" not in manifest.nodes

        assert "model.test.my_model_2" not in manifest.disabled
        assert "model.test.my_model_3" in manifest.disabled


# ensure true config set in yaml file can be overridden by false in sql file
class TestOverrideTrueYAMLConfigsInSQL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_explicit_enabled_yml,
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_enabled,
            "my_model_3.sql": my_model_3_disabled,
        }

    def test_override_yaml_sql_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" in manifest.nodes
        assert "model.test.my_model_3" not in manifest.nodes

        assert "model.test.my_model_2" not in manifest.disabled
        assert "model.test.my_model_3" in manifest.disabled


# ensure error when enabling in schema file when multiple nodes exist within disabled
class TestMultipleDisabledNodesForUniqueIDFailure:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_partial_enabled_yml,
            "my_model.sql": my_model,
            "folder_1": {
                "my_model_2.sql": my_model_2_disabled,
                "my_model_3.sql": my_model_3_disabled,
            },
            "folder_2": {
                "my_model_2.sql": my_model_2_disabled,
                "my_model_3.sql": my_model_3_disabled,
            },
            "folder_3": {
                "my_model_2.sql": my_model_2_disabled,
                "my_model_3.sql": my_model_3_disabled,
            },
        }

    def test_disabled_config(self, project):
        with pytest.raises(ParsingException) as exc:
            run_dbt(["parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "Found 3 matching disabled nodes for 'my_model_2'"
        assert expected_msg in exc_str
