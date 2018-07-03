

{% macro string_literal(s) -%}
  {{ adapter_macro('test.string_literal', s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}

{% macro bigquery__string_literal(s) %}
    cast('{{ s }}' as string)
{% endmacro %}
