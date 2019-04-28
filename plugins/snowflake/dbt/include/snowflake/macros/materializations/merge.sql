{% macro snowflake__get_merge_sql(target, source_sql, unique_key, dest_columns) %}
    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(', ') -%}
        {%- if unique_key is none -%}
            {# workaround for Snowflake not being happy with "on false" merge.
            when no unique key is provided we'll do a regular insert, other times we'll
            use the preferred merge. #}
            insert into {{ target }} ({{ dest_cols_csv }})
            (
                select {{ dest_cols_csv }}
                from {{ source_sql }}
            );
        {%- else -%}
        {# call regular merge when a unique key is present. #}
        {{ common_get_merge_sql(target, source_sql, unique_key, dest_columns) }}
        {%- endif -%}

{% endmacro %}
