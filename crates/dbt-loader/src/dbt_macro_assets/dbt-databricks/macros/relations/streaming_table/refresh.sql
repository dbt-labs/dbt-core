{% macro refresh_streaming_table(relation, sql) -%}
  {{ adapter.dispatch('refresh_streaming_table', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro databricks__refresh_streaming_table(relation, sql) -%}
  create or refresh streaming table {{ relation.render() }}
  as
    {{ sql }}
{% endmacro %}
