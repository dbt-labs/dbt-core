{% macro snowflake__get_merge_sql(target, source_sql, unique_key, dest_columns, predicates) -%}

    {#
       Workaround for Snowflake not being happy with a merge on a constant-false predicate.
       When no unique_key is provided, this macro will do a regular insert. If a unique_key
       is provided, then this macro will do a proper merge instead.
    #}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set dml -%}
    {%- if unique_key is none -%}

        {{ sql_header if sql_header is not none }}

        insert into {{ target }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source_sql }}
        )

    {%- else -%}

        {{ default__get_merge_sql(target, source_sql, unique_key, dest_columns, predicates) }}

    {%- endif -%}
    {%- endset -%}
    
    {% do return(snowflake_dml_explicit_transaction(dml)) %}

{% endmacro %}


{% macro snowflake__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) %}
    {% set dml = default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) %}
    {% do return(snowflake_dml_explicit_transaction(dml)) %}
{% endmacro %}


{% macro snowflake__snapshot_merge_sql(target, source, insert_cols) %}
    {% set dml = default__snapshot_merge_sql(target, source, insert_cols) %}
    {% do return(snowflake_dml_explicit_transaction(dml)) %}
{% endmacro %}
