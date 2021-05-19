{% macro _get_utils_namespaces() %}
  {% set override_namespaces = var('local_utils_dispatch_list', []) %}
  {% do return(override_namespaces + ['local_utils']) %}
{% endmacro %}

{% macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp', packages = local_utils._get_utils_namespaces())()) }}
{%- endmacro %}

{% macro default__current_timestamp() -%}
  now()
{%- endmacro %}
