{% macro snowflake__get_merge_sql(target, source_sql, unique_key, dest_columns) -%}

    {#
       Workaround for Snowflake not being happy with a merge on a constant-false predicate.
       When no unique_key is provided, this macro will do a regular insert. If a unique_key
       is provided, then this macro will do a proper merge instead.
    #}

    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(', ') -%}

    {%- if unique_key is none -%}

        insert into {{ target }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source_sql }}
        );

    {%- else -%}

        {{ common_get_merge_sql(target, source_sql, unique_key, dest_columns) }}

    {%- endif -%}

{% endmacro %}
