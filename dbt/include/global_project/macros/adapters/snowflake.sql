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

{% macro snowflake__create_table_as(temporary, identifier, sql) -%}
  {%- set _clusterby = config.get('clusterby') -%}

  {% if temporary %}
    use schema "{{ schema }}";
  {% endif %}

  create {% if temporary: -%}temporary{%- endif %} table
    {% if not temporary: -%}"{{ schema }}".{%- endif %}"{{ identifier }}" as (
    {{ sql }}
  )
  {{ clusterby(_clusterby) }}
  ;
{% endmacro %}
