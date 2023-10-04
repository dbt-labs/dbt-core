import pytest
from dbt.dataclass_schema import ValidationError
from dbt.tests.util import run_dbt, get_manifest

from dbt.exceptions import CompilationError, ParsingError

from tests.functional.configs.fixtures import (
    schema_all_disabled_yml,
    schema_partial_enabled_yml,
    schema_partial_disabled_yml,
    schema_explicit_enabled_yml,
    schema_invalid_enabled_yml,
    my_model,
    my_model_2,
    my_model_2_enabled,
    my_model_2_disabled,
    my_model_3,
    my_model_3_disabled,
    my_model_3_enabled,
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
        with pytest.raises(CompilationError) as exc:
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
        with pytest.raises(ParsingError) as exc:
            run_dbt(["parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "Found 3 matching disabled nodes for model 'my_model_2'"
        assert expected_msg in exc_str


# ensure error when enabling in schema file when multiple nodes exist within disabled
class TestMultipleDisabledNodesSuccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model,
            "folder_1": {
                "my_model_2.sql": my_model_2,
                "my_model_3.sql": my_model_3,
            },
            "folder_2": {
                "my_model_2.sql": my_model_2,
                "my_model_3.sql": my_model_3,
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "folder_1": {
                        "enabled": False,
                    },
                    "folder_2": {
                        "enabled": True,
                    },
                },
            }
        }

    def test_multiple_disabled_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" in manifest.nodes
        assert "model.test.my_model_3" in manifest.nodes

        expected_file_path = "folder_2"
        assert expected_file_path in manifest.nodes["model.test.my_model_2"].original_file_path
        assert expected_file_path in manifest.nodes["model.test.my_model_3"].original_file_path

        assert "model.test.my_model_2" in manifest.disabled
        assert "model.test.my_model_3" in manifest.disabled

        expected_disabled_file_path = "folder_1"
        assert (
            expected_disabled_file_path
            in manifest.disabled["model.test.my_model_2"][0].original_file_path
        )
        assert (
            expected_disabled_file_path
            in manifest.disabled["model.test.my_model_3"][0].original_file_path
        )


# ensure overrides work when enabling in sql file when multiple nodes exist within disabled
class TestMultipleDisabledNodesOverrideModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model,
            "folder_1": {
                "my_model_2.sql": my_model_2_enabled,
                "my_model_3.sql": my_model_3,
            },
            "folder_2": {
                "my_model_2.sql": my_model_2,
                "my_model_3.sql": my_model_3_enabled,
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "folder_1": {
                        "enabled": False,
                    },
                    "folder_2": {
                        "enabled": False,
                    },
                },
            }
        }

    def test_multiple_disabled_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" in manifest.nodes
        assert "model.test.my_model_3" in manifest.nodes

        expected_file_path_2 = "folder_1"
        assert expected_file_path_2 in manifest.nodes["model.test.my_model_2"].original_file_path
        expected_file_path_3 = "folder_2"
        assert expected_file_path_3 in manifest.nodes["model.test.my_model_3"].original_file_path

        assert "model.test.my_model_2" in manifest.disabled
        assert "model.test.my_model_3" in manifest.disabled

        expected_disabled_file_path_2 = "folder_2"
        assert (
            expected_disabled_file_path_2
            in manifest.disabled["model.test.my_model_2"][0].original_file_path
        )
        expected_disabled_file_path_3 = "folder_1"
        assert (
            expected_disabled_file_path_3
            in manifest.disabled["model.test.my_model_3"][0].original_file_path
        )


# ensure everything lands where it should when disabling multiple nodes with the same unique id
class TestManyDisabledNodesSuccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model,
            "folder_1": {
                "my_model_2.sql": my_model_2,
                "my_model_3.sql": my_model_3,
            },
            "folder_2": {
                "my_model_2.sql": my_model_2,
                "my_model_3.sql": my_model_3,
            },
            "folder_3": {
                "my_model_2.sql": my_model_2,
                "my_model_3.sql": my_model_3,
            },
            "folder_4": {
                "my_model_2.sql": my_model_2,
                "my_model_3.sql": my_model_3,
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "folder_1": {
                        "enabled": False,
                    },
                    "folder_2": {
                        "enabled": True,
                    },
                    "folder_3": {
                        "enabled": False,
                    },
                    "folder_4": {
                        "enabled": False,
                    },
                },
            }
        }

    def test_many_disabled_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "model.test.my_model_2" in manifest.nodes
        assert "model.test.my_model_3" in manifest.nodes

        expected_file_path = "folder_2"
        assert expected_file_path in manifest.nodes["model.test.my_model_2"].original_file_path
        assert expected_file_path in manifest.nodes["model.test.my_model_3"].original_file_path

        assert len(manifest.disabled["model.test.my_model_2"]) == 3
        assert len(manifest.disabled["model.test.my_model_3"]) == 3


class TestInvalidEnabledConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_invalid_enabled_yml,
            "my_model.sql": my_model,
        }

    def test_invalis_config(self, project):
        with pytest.raises(ValidationError) as exc:
            run_dbt(["parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "'True and False' is not of type 'boolean'"
        assert expected_msg in exc_str
