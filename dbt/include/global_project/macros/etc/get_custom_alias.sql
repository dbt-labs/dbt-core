
{#
    Renders a model name given a node. If an `alias` is present in the
    node config, then that alias will be used. If an alias is not provided,
    then the node name is used as a default.

    This macro can be overriden in projects to define different semantics
    for rendering a model alias.

    Arguments:
    node: The node to generate an alias for

#}
{% macro generate_model_alias(node) -%}

    {% set default_alias = node['name'] %}
    {% set config = node.get('config', {}) %}
    {% set alias = config.get('alias', default_alias) %}
    {{ return(alias) }}

{%- endmacro %}
