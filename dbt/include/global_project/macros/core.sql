{% macro statement(name=None, fetch_result=False, auto_begin=True) -%}
  {% set status = None %}
  {% set result = None %}

  {%- if execute: -%}
    {%- set sql = render(caller()) -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
      {{ write(sql) }}
    {%- endif -%}

    {%- set status, result = adapter.execute(sql, auto_begin=auto_begin, fetch=fetch_result) -%}
  {%- endif -%}

  {%- if name is not none -%}
    {{ store_result(name, status=status, agate_table=result) }}
  {%- endif -%}

{%- endmacro %}

{% macro noop_statement(name=None, status=None, res=None) -%}
  {%- set sql = render(caller()) -%}

  {%- if name == 'main' -%}
    {{ log('Writing runtime SQL for node "{}"'.format(model['unique_id'])) }}
    {{ write(sql) }}
  {%- endif -%}

  {%- if name is not none -%}
    {{ store_result(name, status=status, agate_table=res) }}
  {%- endif -%}

{%- endmacro %}
