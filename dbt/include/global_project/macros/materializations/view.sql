
{% macro dbt__create_view(schema, identifier, sql) -%}

    create view {{ schema }}.{{ identifier }} as (
        {{ sql }}
    );

{%- endmacro %}
