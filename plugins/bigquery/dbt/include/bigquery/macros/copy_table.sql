{# copy_table macro allows for a table copy job to be submitted to BigQuery, which should run
 # faster than `select * from`. The macro takes in a BigQueryRelation (returned by a ref() or
 # source()) defining the source table for the copy, which is forwarded to the adapter. If the
 # materialization is set to table, creates a table or overwrites and existing one. If incremental,
 # appends to the existing table. Other materializations are not supported, and throw an error.
 #}
{% macro copy_table(source) %}

  {%- if not execute: -%}
    {%- set materialized_method = config.source_config['config'].get('materialized', '') -%}
    {{ config(copy_materialization=materialized_method) }}
    {%- if materialized_method not in ('table', 'incremental') -%}
      {{
        exceptions.raise_not_implemented(
          'Copy must materialize as table or incremental, not %s' %
          materialized_method)
      }}
    {%- endif -%}
  {%- endif -%}

  {{ config(materialized='copy') }}

  {%- set destination = api.Relation.create(
      database=database, schema=schema, identifier=model['alias'], type='table') -%}

  {{
    adapter.copy_table(
      source,
      destination,
      config.get('copy_materialization'))
  }}
