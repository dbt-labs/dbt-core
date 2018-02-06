{% macro azure_dist(dist) %}
  {%- if dist is not none -%}
    {%- set dist = dist.strip() -%}
    distribution = {{ dist }}
  {%- else -%}
    distribution = round_robin
  {%- endif -%}
{%- endmacro -%}

{% macro statistics(fields) %}
  {%- if fields is not none -%}
    {% for field in fields %}
      create statistics dbt_stats_{{this.name}}_{{field}} on {{this}} ({{field}});
    {% endfor %}
  {%- endif -%}
{% endmacro %}

{% macro azure_rowstore(rowstore) %}
  {%- if rowstore is not none -%}
    {{ rowstore }}
  {%- else -%}
    clustered columnstore index
  {%- endif -%}
{% endmacro %}

{% macro azure_dw__create_view_as(identifier, sql) -%}
  create view {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }} as
  {{ hoist_ctes(sql) }}
{% endmacro %}

{% macro azure_dw__create_table_as(temporary, identifier, sql) -%}

  {%- set _dist = config.get('distribution') -%}
  {%- set _statistics = config.get('statistics') -%}
  {%- set _rowstore = config.get('rowstore') -%}

  create table {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }}
  with (
    {{ ",".join([ azure_rowstore(_rowstore), azure_dist(_dist) ]) }}
  )
  as {{ hoist_ctes(sql) }};

  {{ statistics(_statistics) }}

  alter index all on {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }} rebuild;

  update statistics {{ adapter.quote(schema) }}.{{ adapter.quote(identifier) }};

{% endmacro %}
