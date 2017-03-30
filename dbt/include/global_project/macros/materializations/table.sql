
{% macro dbt__create_table(schema, identifier, dist, sort, sql, flags, funcs) -%}

    {% if not flags.NON_DESTRUCTIVE or
          not funcs['already_exists'](schema, identifier) -%}
        create table {{ schema }}.{{ identifier }} {{ dist }} {{ sort }} as (
            {{ sql }}
        );
    {%- else -%}
        create temporary table {{ identifier }}__dbt_tmp {{ dist }} {{ sort }} as (
            {{ sql }}
        );

        {% set dest_columns = funcs['get_columns_in_table'](schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ schema }}.{{ identifier }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from "{{ identifier }}__dbt_tmp"
        );
    {%- endif %}

{%- endmacro %}
