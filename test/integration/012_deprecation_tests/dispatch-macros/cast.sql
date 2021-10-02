

{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', packages=['test'])(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}

