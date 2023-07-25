seed_csv = """id,name
1,Alice
2,Bob
"""

table_model_sql = """
{{ config(materialized='table') }}
select * from {{ ref('ephemeral_model') }}

-- establish a macro dependency to trigger state:modified.macros
-- depends on: {{ my_macro() }}
"""

view_model_sql = """
select * from {{ ref('seed') }}

-- establish a macro dependency that trips infinite recursion if not handled
-- depends on: {{ my_infinitely_recursive_macro() }}
"""

ephemeral_model_sql = """
{{ config(materialized='ephemeral') }}
select * from {{ ref('view_model') }}
"""

exposures_yml = """
version: 2
exposures:
  - name: my_exposure
    type: application
    depends_on:
      - ref('view_model')
    owner:
      email: test@example.com
"""

schema_yml = """
version: 2
models:
  - name: view_model
    columns:
      - name: id
        tests:
          - unique:
              severity: error
          - not_null
      - name: name
"""

get_schema_name_sql = """
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is not none -%}
        {{ return(default_schema ~ '_' ~ custom_schema_name|trim) }}
    -- put seeds into a separate schema in "prod", to verify that cloning in "dev" still works
    {%- elif target.name == 'default' and node.resource_type == 'seed' -%}
        {{ return(default_schema ~ '_' ~ 'seeds') }}
    {%- else -%}
        {{ return(default_schema) }}
    {%- endif -%}
{%- endmacro %}
"""

snapshot_sql = """
{% snapshot my_cool_snapshot %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['id'],
        )
    }}
    select * from {{ ref('view_model') }}

{% endsnapshot %}
"""
macros_sql = """
{% macro my_macro() %}
    {% do log('in a macro' ) %}
{% endmacro %}
"""

infinite_macros_sql = """
{# trigger infinite recursion if not handled #}

{% macro my_infinitely_recursive_macro() %}
  {{ return(adapter.dispatch('my_infinitely_recursive_macro')()) }}
{% endmacro %}

{% macro default__my_infinitely_recursive_macro() %}
    {% if unmet_condition %}
        {{ my_infinitely_recursive_macro() }}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}
"""

custom_can_clone_tables_false_macros_sql = """
{% macro can_clone_table() %}
    {{ return(False) }}
{% endmacro %}
"""
