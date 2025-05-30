{% macro current_catalog() -%}
  {{ return(adapter.dispatch('current_catalog', 'dbt')()) }}
{% endmacro %}

{% macro databricks__current_catalog() -%}
  {% call statement('current_catalog', fetch_result=True) %}
      select current_catalog()
  {% endcall %}
  {% do return(load_result('current_catalog').table) %}
{% endmacro %}

{% macro use_catalog(catalog) -%}
  {{ return(adapter.dispatch('use_catalog', 'dbt')(catalog)) }}
{% endmacro %}

{% macro databricks__use_catalog(catalog) -%}
  {% call statement() %}
    use catalog {{ adapter.quote(catalog)|lower }}
  {% endcall %}
{% endmacro %}
