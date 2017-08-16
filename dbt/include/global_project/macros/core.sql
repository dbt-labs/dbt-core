{% macro statement(name=None, fetch_result=False, auto_begin=True) -%}
  {%- if execute: -%}
    {%- set sql = render(caller()) -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
      {{ write(sql) }}
    {%- endif -%}

    {%- set _, cursor = adapter.add_query(sql, auto_begin=auto_begin) -%}
    {%- if name is not none -%}
      {%- set result = [] if not fetch_result else adapter.get_result_from_cursor(cursor) -%}
      {{ store_result(name, status=adapter.get_status(cursor), data=result) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
