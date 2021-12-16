
{#-- Verify that the config['alias'] key is present #}
{% macro generate_alias_name(custom_alias_name, node) -%}
    {%- if custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        custom_{{ node.config['alias'] if 'alias' in node.config else '' | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}
