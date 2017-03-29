
{% macro dbt__create_table(schema, identifier, dist, sort, sql) -%}

    create table {{ schema }}.{{ identifier }} {{ dist }} {{ sort }} as (
        {{ sql }}
    );

{%- endmacro %}
