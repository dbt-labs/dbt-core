{% macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp', packages = local_utils._get_utils_namespaces())()) }}
{%- endmacro %}

{% macro default__current_timestamp() -%}
  now()
{%- endmacro %}
