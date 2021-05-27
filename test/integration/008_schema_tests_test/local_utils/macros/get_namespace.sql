{% macro _get_utils_namespaces() %}
  {% set override_namespaces = var('local_utils_dispatch_list', []) %}
  {% do return(override_namespaces + ['local_utils']) %}
{% endmacro %}
