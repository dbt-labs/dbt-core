

{# this shouldnt be duplicated - use the existing macro! #}
{% macro dbt__create_table(schema, identifier, dist, sort, sql) -%}

    create table {{ schema }}.{{ identifier }} {{ dist }} {{ sort }} as (
        {{ sql }}
    );

{%- endmacro %}

{% macro dbt__incremental_delete(schema, identifier, unique_key) -%}

    delete
    from "{{ schema }}"."{{ identifier }}"
    where ({{ unique_key }}) in (
        select ({{ unique_key }}) from "{{ identifier }}__dbt_incremental_tmp"
    );

{%- endmacro %}

{% macro dbt__create_incremental(schema, identifier, dist, sort, sql, sql_where, funcs, unique_key=None) -%}

    {% if not funcs['already_exists'](schema, identifier) -%}

        {{ dbt__create_table(schema, identifier, dist, sort, sql) }}

    {%- else -%}

        create temporary table "{{ identifier }}__dbt_incremental_tmp" as (
            with dbt_incr_sbq as (
                {{ sql }}
            )
            select * from dbt_incr_sbq
            where coalesce({{ sql_where }}, TRUE)
        );

        -- DBT_OPERATION { function: expand_column_types_if_needed, args: { temp_table: "{{ identifier }}__dbt_incremental_tmp", to_schema: "{{ schema }}", to_table: "{{ identifier }}"} }

        {% set dest_columns = funcs['get_columns_in_table'](schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        {% if unique_key is defined -%}

            {{ dbt__incremental_delete(schema, identifier, unique_key) }}

        {%- endif %}

        insert into "{{ schema }}"."{{ identifier }}" ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from "{{ identifier }}__dbt_incremental_tmp"
        );

    {%- endif %}

{%- endmacro %}
