import pytest
from dbt.tests.util import run_dbt


models_dupe_custom_database__schema_yml = """
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

models_dupe_custom_database__model_b_sql = """
select {{ string_literal(this.name) }} as tablename

"""

models_dupe_custom_database__model_a_sql = """
select {{ string_literal(this.name) }} as tablename

"""

models_dupe_custom_database__README_md = """
these should succeed, as both models have the same alias,
but they are configured to be built in _different_ schemas

"""

models_dupe__model_b_sql = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

models_dupe__model_a_sql = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

models_dupe__README_md = """
these should fail because both models have the same alias
and are configured to build in the same schema

"""

models__schema_yml = """
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

models__foo_alias_sql = """

{{
    config(
        alias='foo',
        materialized='table'
    )
}}

select {{ string_literal(this.name) }} as tablename

"""

models__alias_in_project_sql = """

select {{ string_literal(this.name) }} as tablename

"""

models__alias_in_project_with_override_sql = """

{{ config(alias='override_alias') }}

select {{ string_literal(this.name) }} as tablename

"""

models__ref_foo_alias_sql = """

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

macros__cast_sql = """


{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}

"""

macros__expect_value_sql = """

-- cross-db compatible test, similar to accepted_values

{% test expect_value(model, field, value) %}

select *
from {{ model }}
where {{ field }} != '{{ value }}'

{% endtest %}

"""

models_dupe_custom_schema__schema_yml = """
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

models_dupe_custom_schema__model_c_sql = """

-- no custom schema for this model
{{ config(alias='duped_alias') }}

select {{ string_literal(this.name) }} as tablename

"""

models_dupe_custom_schema__model_b_sql = """

{{ config(alias='duped_alias', schema='schema_b') }}

select {{ string_literal(this.name) }} as tablename

"""

models_dupe_custom_schema__model_a_sql = """

{{ config(alias='duped_alias', schema='schema_a') }}

select {{ string_literal(this.name) }} as tablename

"""

models_dupe_custom_schema__README_md = """
these should succeed, as both models have the same alias,
but they are configured to be built in _different_ schemas

"""


class TestAliases:
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
            "schema.yml": models__schema_yml,
            "foo_alias.sql": models__foo_alias_sql,
            "alias_in_project.sql": models__alias_in_project_sql,
            "alias_in_project_with_override.sql": models__alias_in_project_with_override_sql,
            "ref_foo_alias.sql": models__ref_foo_alias_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": macros__cast_sql, "expect_value.sql": macros__expect_value_sql}

    def test_alias_model_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4
        run_dbt(["test"])


class TestAliasErrors:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": macros__cast_sql, "expect_value.sql": macros__expect_value_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_b.sql": models_dupe__model_b_sql,
            "model_a.sql": models_dupe__model_a_sql,
            "README.md": models_dupe__README_md,
        }

    def test_alias_dupe_thorews_exeption(self, project):
        message = ".*identical database representation.*"
        with self.assertRaisesRegex(Exception, message):
            run_dbt(["run"])


class TestSameAliasDifferentSchemas:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": macros__cast_sql, "expect_value.sql": macros__expect_value_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_dupe_custom_schema__schema_yml,
            "model_c.sql": models_dupe_custom_schema__model_c_sql,
            "model_b.sql": models_dupe_custom_schema__model_b_sql,
            "model_a.sql": models_dupe_custom_schema__model_a_sql,
            "README.md": models_dupe_custom_schema__README_md,
        }

    def test_same_alias_succeeds_in_different_schemas(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        res = run_dbt(["test"])
        assert len(res) > 0


class TestSameAliasDifferentDatabases:
    setup_alternate_db = True

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
            "models": {
                "test": {
                    "alias": "duped_alias",
                    "model_b": {
                        "database": self.alternate_db,
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"cast.sql": macros__cast_sql, "expect_value.sql": macros__expect_value_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_dupe_custom_database__schema_yml,
            "model_b.sql": models_dupe_custom_database__model_b_sql,
            "model_a.sql": models_dupe_custom_database__model_a_sql,
            "README.md": models_dupe_custom_database__README_md,
        }
