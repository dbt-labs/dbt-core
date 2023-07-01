{% macro dry_run_query(test_sql) -%}
  {{ return(adapter.dispatch('dry_run_query', 'dbt')(test_sql)) }}
{% endmacro %}

{% macro default__dry_run_query(test_sql) -%}
  {% call statement('dry_run_query') -%}
    explain {{ test_sql }}
  {% endcall %}
  {{ return(load_result('dry_run_query')) }}
{% endmacro %}
