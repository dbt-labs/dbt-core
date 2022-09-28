# macros #
MACROS__CAST_SQL = """


{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}

"""

MACROS__EXPECT_VALUE_SQL = """

-- cross-db compatible test, similar to accepted_values

{% test expect_value(model, field, value) %}

select *
from {{ model }}
where {{ field }} != '{{ value }}'

{% endtest %}

"""

# base aliases #
MODELS__SCHEMA_YML = """
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

MODELS__FOO_ALIAS_SQL = """

{{
    config(
        alias='foo',
        materialized='table'
    )
}}

select {{ string_literal(this.name) }} as tablename

"""

MODELS__ALIAS_IN_PROJECT_SQL = """

select {{ string_literal(this.name) }} as tablename

"""

MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL = """

{{ config(alias='override_alias') }}

select {{ string_literal(this.name) }} as tablename

"""

MODELS__REF_FOO_ALIAS_SQL = """

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

# error #
MODELS_DUPE__MODEL_A_SQL = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

MODELS_DUPE__MODEL_B_SQL = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

MODELS_DUPE__README_MD = """
these should fail because both models have the same alias
and are configured to build in the same schema

"""

# dupe custom database #
MODELS_DUPE_CUSTOM_DATABASE__SCHEMA_YML = """
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

MODELS_DUPE_CUSTOM_DATABASE__MODEL_A_SQL = """
select {{ string_literal(this.name) }} as tablename

"""

MODELS_DUPE_CUSTOM_DATABASE__MODEL_B_SQL = """
select {{ string_literal(this.name) }} as tablename

"""

MODELS_DUPE_CUSTOM_DATABASE__README_MD = """
these should succeed, as both models have the same alias,
but they are configured to be built in _different_ schemas

"""

# dupe custom schema #
MODELS_DUPE_CUSTOM_SCHEMA__SCHEMA_YML = """
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

MODELS_DUPE_CUSTOM_SCHEMA__MODEL_A_SQL = """

{{ config(alias='duped_alias', schema='schema_a') }}

select {{ string_literal(this.name) }} as tablename

"""

MODELS_DUPE_CUSTOM_SCHEMA__MODEL_B_SQL = """

{{ config(alias='duped_alias', schema='schema_b') }}

select {{ string_literal(this.name) }} as tablename

"""

MODELS_DUPE_CUSTOM_SCHEMA__MODEL_C_SQL = """

-- no custom schema for this model
{{ config(alias='duped_alias') }}

select {{ string_literal(this.name) }} as tablename

"""

MODELS_DUPE_CUSTOM_SCHEMA__README_MD = """
these should succeed, as both models have the same alias,
but they are configured to be built in _different_ schemas

"""
