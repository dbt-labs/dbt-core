{#-- 
TODO:  Only certian adapters need to support python models so this should probably be 
moved to an override I.E. `spark__statement`  but for some reason it wasn't picking up that change
when implemented in dbt-spark/dbt/include/spark/macros/adapters.sql
--#}

{%- macro statement(name=None, fetch_result=False, auto_begin=True, language='sql') -%}
  {%- if execute: -%}
    {%- set model_code = caller() -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime {} for node "{}"'.format(language, model['unique_id'])) }}
      {{ write(model_code) }}
    {%- endif -%}

    {%- if language == 'sql'-%}
      {%- set res, table = adapter.execute(model_code, auto_begin=auto_begin, fetch=fetch_result) -%}
    {%- elif language == 'python' -%}
      {%- set res = adapter.submit_python_job(schema, model['alias'], model_code) -%}
      {#-- TODO: What should table be for python models?--#}
      {%- set table = None -%}
    {%- endif -%}

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
