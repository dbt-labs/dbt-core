model1_sql = """
{{ config(materialized='table', alias='alias') }}

select {{ string_literal(this.name) }} as model_name
"""

model2_sql = """
{{ config(materialized='table') }}

select {{ string_literal(this.name) }} as model_name
"""

macros_sql = """
{% macro generate_alias_name(custom_alias_name, node) -%}
    {%- if custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        custom_{{ custom_alias_name | trim }}
    {%- endif -%}
{%- endmacro %}


{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}
"""

macros_config_sql = """
{#-- Verify that the config['alias'] key is present #}
{% macro generate_alias_name(custom_alias_name, node) -%}
    {%- if custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        custom_{{ node.config['alias'] if 'alias' in node.config else '' | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}
"""

schema_yml = """
version: 2

models:
  - name: model1
    columns:
      - name: model_name
        tests:
          - accepted_values:
             values: ['custom_alias']
  - name: model2
    columns:
      - name: model_name
        tests:
          - accepted_values:
             values: ['model2']

"""
