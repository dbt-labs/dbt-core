{% macro test_pkg_and_dispatch(model) -%}
  {{ return(adapter.dispatch('test_pkg_and_dispatch', packages = local_utils._get_utils_namespaces())()) }}
{%- endmacro %}

{% macro default__test_pkg_and_dispatch(model) %}
    select {{ local_utils.current_timestamp() }}
{% endmacro %}
