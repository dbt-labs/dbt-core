
{% macro generate_model_alias(node) -%}

    {% set default_alias = node['name'] %}
    {% set config = node.get('config', {}) %}
    {% set alias = config.get('alias', default_alias) %}
    {{ return(alias ~ "_ALIAS") }}

{%- endmacro %}
