{% macro snowflake__generate_database_name(custom_database_name=none, node=none) -%}
    {# -- adapter in Core is Snowflake, here it's the ParseAdapter #}
    {%- if custom_database_name is none -%}
        {%- if node is not none and node|attr('database') -%}
            {%- set catalog_relation = adapter.build_catalog_relation(node) -%}
        {%- elif 'config' in target -%}
            {%- set catalog_relation = adapter.build_catalog_relation(target) -%}
        {%- else -%}
            {%- set catalog_relation = none -%}
        {%- endif -%}
        {%- if catalog_relation is not none
            and catalog_relation|attr('catalog_linked_database')-%}
            {{ return(catalog_relation.catalog_linked_database) }}
        {%- else -%}
            {{ target.database }}
        {%- endif -%}
    {%- else -%}
       {{ custom_database_name }}
    {%- endif -%}
{%- endmacro %}
