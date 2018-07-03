

{% macro cast_to_string(expr) -%}
  {{ adapter_macro('test.cast_to_string') }}
{%- endmacro %}

{% macro default__cast_to_string(expr) %}
    {{ expr }}::text
{% endmacro %}

{% macro bigquery__cast_to_string(expr) %}
    cast({{ expr }} as string)
{% endmacro %}
