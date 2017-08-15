{% materialization view, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- if existing_type is not none -%}
    {{ adapter.drop(identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% set result = adapter.execute_model(model, 'view') %}
  {{ store_result('main', status=result) }}

{%- endmaterialization %}

{% materialization table, adapter='bigquery' -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {%- if existing_type is not none -%}
    {{ adapter.drop(identifier, existing_type) }}
  {%- endif -%}

  -- build model
  {% set result = adapter.execute_model(model, 'table') %}
  {{ store_result('main', status=result) }}

{% endmaterialization %}

{% materialization incremental, adapter='bigquery' -%}

  {{ exceptions.materialization_not_available(model, 'bigquery') }}

{% endmaterialization %}
