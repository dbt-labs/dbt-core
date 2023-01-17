import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.aliases.fixtures import (
    MACROS__CAST_SQL,
    MACROS__EXPECT_VALUE_SQL,
    MODELS__SCHEMA_YML,
    MODELS__FOO_ALIAS_SQL,
    MODELS__ALIAS_IN_PROJECT_SQL,
    MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL,
    MODELS__REF_FOO_ALIAS_SQL,
    MODELS_DUPE__MODEL_A_SQL,
    MODELS_DUPE__MODEL_B_SQL,
    MODELS_DUPE_CUSTOM_SCHEMA__SCHEMA_YML,
    MODELS_DUPE_CUSTOM_SCHEMA__MODEL_A_SQL,
    MODELS_DUPE_CUSTOM_SCHEMA__MODEL_B_SQL,
    MODELS_DUPE_CUSTOM_SCHEMA__MODEL_C_SQL,
    MODELS_DUPE_CUSTOM_DATABASE__SCHEMA_YML,
    MODELS_DUPE_CUSTOM_DATABASE__MODEL_A_SQL,
    MODELS_DUPE_CUSTOM_DATABASE__MODEL_B_SQL,
)


class BaseAliases:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
            "models": {
                "test": {
                    "alias_in_project": {
                        "alias": "project_alias",
                    },
                    "alias_in_project_with_override": {
                        "alias": "project_alias",
                    },
                }
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": MODELS__SCHEMA_YML,
            "foo_alias.sql": MODELS__FOO_ALIAS_SQL,
            "alias_in_project.sql": MODELS__ALIAS_IN_PROJECT_SQL,
            "alias_in_project_with_override.sql": MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL,
            "ref_foo_alias.sql": MODELS__REF_FOO_ALIAS_SQL,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": MACROS__CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}

    def test_alias_model_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4
        run_dbt(["test"])


class BaseAliasErrors:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": MACROS__CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": MODELS_DUPE__MODEL_A_SQL,
            "model_b.sql": MODELS_DUPE__MODEL_B_SQL,
        }

    def test_alias_dupe_thorews_exeption(self, project):
        message = ".*identical database representation.*"
        with pytest.raises(Exception) as exc:
            assert message in exc
            run_dbt(["run"])


class BaseSameAliasDifferentSchemas:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": MACROS__CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": MODELS_DUPE_CUSTOM_SCHEMA__SCHEMA_YML,
            "model_a.sql": MODELS_DUPE_CUSTOM_SCHEMA__MODEL_A_SQL,
            "model_b.sql": MODELS_DUPE_CUSTOM_SCHEMA__MODEL_B_SQL,
            "model_c.sql": MODELS_DUPE_CUSTOM_SCHEMA__MODEL_C_SQL,
        }

    def test_same_alias_succeeds_in_different_schemas(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        res = run_dbt(["test"])
        assert len(res) > 0


class BaseSameAliasDifferentDatabases:
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
            "models": {
                "test": {
                    "alias": "duped_alias",
                    "model_b": {"schema": unique_schema + "_alt"},
                },
            },
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": MACROS__CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": MODELS_DUPE_CUSTOM_DATABASE__SCHEMA_YML,
            "model_a.sql": MODELS_DUPE_CUSTOM_DATABASE__MODEL_A_SQL,
            "model_b.sql": MODELS_DUPE_CUSTOM_DATABASE__MODEL_B_SQL,
        }

    def test_alias_model_name_diff_database(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        res = run_dbt(["test"])
        assert len(res) > 0


class TestAliases(BaseAliases):
    pass


class TestAliasErrors(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    pass
