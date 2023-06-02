{% macro set_sql_header(config) -%}
  {{ config.set('sql_header', caller()) }}
{%- endmacro %}


{% macro should_full_refresh() %}
  {% set config_full_refresh = config.get('full_refresh') %}
  {% if config_full_refresh is none %}
    {% set config_full_refresh = flags.FULL_REFRESH %}
  {% endif %}
  {% do return(config_full_refresh) %}
{% endmacro %}


{% macro should_store_failures() %}
  {% set config_store_failures = config.get('store_failures') %}
  {% if config_store_failures is none %}
    {% set config_store_failures = flags.STORE_FAILURES %}
  {% endif %}
  {% do return(config_store_failures) %}
{% endmacro %}

{% macro set_query_tag() %}
  {{ return(adapter.dispatch('set_query_tag', 'dbt')()) }}
{% endmacro %}

{% macro unset_query_tag(original_query_tag) %}
  {{ return(adapter.dispatch('unset_query_tag', 'dbt')(original_query_tag)) }}
{% endmacro %}

{% macro default__set_query_tag() %}
  {{ log("Couldn't set query tag, current adapter does not have get_current_query_tag() macro implemented.",info=True) }}
{% endmacro%}

{% macro default__unset_query_tag() %}
  {{ log("Couldn't unset query tag, current adapter does not have get_current_query_tag() macro implemented.",info=True) }}
{% endmacro%}