
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- set node_name = node.name | trim -%}
    {%- set split_name = node_name.split('.') -%}
    {%- set n_parts = split_name | length -%}
    
    {{ split_name[1] if n_parts>1 else node_name }}

{%- endmacro -%}


{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}
    {%- set node_name = node.name | trim -%}
    {%- set split_name = node_name.split('.') -%}
    {%- set n_parts = split_name | length -%}
    
    {{ split_name[0] if n_parts>1 else default_schema }}

{%- endmacro -%}
