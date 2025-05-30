{# executes a query and explicitly drops the staging table. #}
{% macro statement_with_staging_table(name=None, staging_table=None, fetch_result=False, auto_begin=True) -%}
  {%- if execute: -%}
    {%- set sql = caller() -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
      {{ write(sql) }}
    {%- endif -%}

    {%- set res, table = adapter.execute(sql, auto_begin=auto_begin, fetch=fetch_result, staging_table=staging_table) -%}
    {%- if name is not none -%}
      {{ store_result(name, response=res, agate_table=table) }}
    {%- endif -%}

  {%- endif -%}
{%- endmacro %}
