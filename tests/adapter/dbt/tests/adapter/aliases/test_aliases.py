import pytest
from dbt.tests.util import run_dbt


_MODELS_DUPE_CUSTOM_DATABASE__SCHEMA_YML = """
version: 2
models:
- name: model_a
  tests:
  - expect_value:
      field: tablename
      value: duped_alias
- name: model_b
  tests:
  - expect_value:
      field: tablename
      value: duped_alias

"""

_MODELS_DUPE_CUSTOME_DATABASE__MODEL_B_SQL = """
select {{ string_literal(this.name) }} as tablename

"""

_MODELS_DUPE_CUSTOME_DATABASE__MODEL_A_SQL = """
select {{ string_literal(this.name) }} as tablename

"""

_MODELS_DUPE_CUSTOM_DATABASE__README_MD = """
these should succeed, as both models have the same alias,
but they are configured to be built in _different_ schemas

"""

_MODELS_DUPE__MODEL_B_SQL = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

_MODELS_DUPE__MODEL_A_SQL = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

_MODELS_DUPE__README_MD = """
these should fail because both models have the same alias
and are configured to build in the same schema

"""

_MODELS__SCHEMA_YML = """
version: 2
models:
- name: foo_alias
  tests:
  - expect_value:
      field: tablename
      value: foo
- name: ref_foo_alias
  tests:
  - expect_value:
      field: tablename
      value: ref_foo_alias
- name: alias_in_project
  tests:
  - expect_value:
      field: tablename
      value: project_alias
- name: alias_in_project_with_override
  tests:
  - expect_value:
      field: tablename
      value: override_alias

"""

_MODELS__FOO_ALIAS_SQL = """

{{
    config(
        alias='foo',
        materialized='table'
    )
}}

select {{ string_literal(this.name) }} as tablename

"""

_MODELS__ALIAS_IN_PROJECT_SQL = """

select {{ string_literal(this.name) }} as tablename

"""

_MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL = """

{{ config(alias='override_alias') }}

select {{ string_literal(this.name) }} as tablename

"""

_MODELS__REF_FOO_ALIAS_SQL = """

{{
    config(
        materialized='table'
    )
}}

with trigger_ref as (

  -- we should still be able to ref a model by its filepath
  select * from {{ ref('foo_alias') }}

)

-- this name should still be the filename
select {{ string_literal(this.name) }} as tablename

"""


_MACROS__CAST_SQL = """


{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}

"""

_MACROS__EXPECT_VALUE_SQL = """

-- cross-db compatible test, similar to accepted_values

{% test expect_value(model, field, value) %}

select *
from {{ model }}
where {{ field }} != '{{ value }}'

{% endtest %}

"""

_MODELS_DUPE_CUSTOM_SCHEMA__SCHEMA_YML = """
version: 2
models:
- name: model_a
  tests:
  - expect_value:
      field: tablename
      value: duped_alias
- name: model_b
  tests:
  - expect_value:
      field: tablename
      value: duped_alias
- name: model_c
  tests:
  - expect_value:
      field: tablename
      value: duped_alias

"""

_MODELS_DUPE_CUSTOM_SCHEMA__MODEL_C_SQL = """

-- no custom schema for this model
{{ config(alias='duped_alias') }}

select {{ string_literal(this.name) }} as tablename

"""

_MODELS_DUPE_CUSTOM_SCHEMA__MODEL_B_SQL = """

{{ config(alias='duped_alias', schema='schema_b') }}

select {{ string_literal(this.name) }} as tablename

"""

_MODELS_DUPE_CUSTOM_SCHEMA__MODEL_A_SQL = """

{{ config(alias='duped_alias', schema='schema_a') }}

select {{ string_literal(this.name) }} as tablename

"""

_MODELS_DUPE_CUSTOM_SCHEMA__README_MD = """
these should succeed, as both models have the same alias,
but they are configured to be built in _different_ schemas

"""


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
            "schema.yml": _MODELS__SCHEMA_YML,
            "foo_alias.sql": _MODELS__FOO_ALIAS_SQL,
            "alias_in_project.sql": _MODELS__ALIAS_IN_PROJECT_SQL,
            "alias_in_project_with_override.sql": _MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL,
            "ref_foo_alias.sql": _MODELS__REF_FOO_ALIAS_SQL,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": _MACROS__CAST_SQL, "expect_value.sql": _MACROS__EXPECT_VALUE_SQL}

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
        return {"cast.sql": _MACROS__CAST_SQL, "expect_value.sql": _MACROS__EXPECT_VALUE_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_b.sql": _MODELS_DUPE__MODEL_B_SQL,
            "model_a.sql": _MODELS_DUPE__MODEL_A_SQL,
            "README.md": _MODELS_DUPE__README_MD,
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
        return {"cast.sql": _MACROS__CAST_SQL, "expect_value.sql": _MACROS__EXPECT_VALUE_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _MODELS_DUPE_CUSTOM_SCHEMA__SCHEMA_YML,
            "model_c.sql": _MODELS_DUPE_CUSTOM_SCHEMA__MODEL_C_SQL,
            "model_b.sql": _MODELS_DUPE_CUSTOM_SCHEMA__MODEL_B_SQL,
            "model_a.sql": _MODELS_DUPE_CUSTOM_SCHEMA__MODEL_A_SQL,
            "README.md": _MODELS_DUPE_CUSTOM_SCHEMA__README_MD,
        }

    def test_same_alias_succeeds_in_different_schemas(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        res = run_dbt(["test"])
        assert len(res) > 0


class BaseSameAliasDifferentDatabases:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        alternate_schema_name = project.test_schema + "_alt_test"
        project.create_test_schema(schema_name=alternate_schema_name)

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        alternate_schema = unique_schema + "_test"
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
            "models": {
                "test": {
                    "alias": "duped_alias",
                    "model_b": {
                        # how to tap into a alternate database here? or is something like dbt_profile_target method required
                        "schema": alternate_schema
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": _MACROS__CAST_SQL, "expect_value.sql": _MACROS__EXPECT_VALUE_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _MODELS_DUPE_CUSTOM_DATABASE__SCHEMA_YML,
            "model_b.sql": _MODELS_DUPE_CUSTOME_DATABASE__MODEL_B_SQL,
            "model_a.sql": _MODELS_DUPE_CUSTOME_DATABASE__MODEL_A_SQL,
            "README.md": _MODELS_DUPE_CUSTOM_DATABASE__README_MD,
        }

    def test_alias_model_name_diff_database(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        run_dbt(["test"])


class TestAliases(BaseAliases):
    pass


class TestAliasErrors(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    pass
