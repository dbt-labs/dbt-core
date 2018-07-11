{% macro clusterby(_clusterby) %}
  {%- if _clusterby is not none -%}
    cluster by (
      {%- if _clusterby is string -%}
        {%- set _clusterby = [_clusterby] -%}
      {%- endif -%}
      {%- for item in _clusterby -%}
        "{{ item }}"
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
    )
  {%- endif -%}
{%- endmacro -%}

{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {%- set _clusterby = config.get('clusterby') -%}

  {% if temporary %}
    use schema {{ schema }};
  {% endif %}

  {# FIXME: We cannot call default__create_table_as here
            as it terminates the return string with a semicolon.
            Conversely, we have introduced code-copy as
            a potential source of drift. #}
  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(schema=(not temporary)) }}
  as (
    {{ sql }}
  )
  {{ clusterby(_clusterby) }}
  ;
{% endmacro %}
