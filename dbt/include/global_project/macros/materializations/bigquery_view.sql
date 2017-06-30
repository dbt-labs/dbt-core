{% materialization view, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  -- setup
  {%- if existing_type is not none -%}
    {{ adapter.drop(identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {{ statement_result_callback(adapter.execute_model(model)) }}

{%- endmaterialization %}
