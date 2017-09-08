{% macro statement(name=None, fetch_result=False, auto_begin=True) -%}
  {%- if execute: -%}
    {%- set sql = render(caller()) -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
      {{ write(sql) }}
    {%- endif -%}

    {%- set res = adapter.execute_and_fetch(sql) -%}
    {%- if name is not none -%}
      {{ store_result(name, status='ok', data=res) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
