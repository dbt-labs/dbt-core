{% macro statement(name=None, fetch_result=False, auto_begin=True) -%}
  {%- if execute: -%}
    {%- set sql = caller() -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
      {{ write(sql) }}
    {%- endif -%}

    {%- set res, table = adapter.execute(sql, auto_begin=auto_begin, fetch=fetch_result) -%}
    {%- if name is not none -%}
      {{ store_result(name, response=res, agate_table=table) }}
    {%- endif -%}

  {%- endif -%}
{%- endmacro %}


{% macro noop_statement(name=None, message=None, code=None, rows_affected=None, res=None) -%}
  {%- set sql = caller() -%}

  {%- if name == 'main' -%}
    {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
    {{ write(sql) }}
  {%- endif -%}

  {%- if name is not none -%}
    {{ store_raw_result(name, message=message, code=code, rows_affected=rows_affected, agate_table=res) }}
  {%- endif -%}

{%- endmacro %}


{# a user-friendly interface into statements #}
{% macro run_query(sql) %}
  {% call statement("run_query_statement", fetch_result=true, auto_begin=false) %}
    {{ sql }}
  {% endcall %}

  {% do return(load_result("run_query_statement").table) %}
{% endmacro %}
