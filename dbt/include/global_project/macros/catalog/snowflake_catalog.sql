
{% macro snowflake__get_catalog() -%}

    {%- call statement('catalog', fetch_result=True) -%}

    with tables as (

        select
            table_schema as "table_schema",
            table_name as "table_name",
            table_type as "table_type",

            -- note: this is the _role_ that owns the table
            table_owner as "table_owner",

            true as "stats_has_stats",
            clustering_key as "stats_clustering_key",
            row_count as "stats_row_count",
            bytes as "stats_bytes"

        from information_schema.tables

    ),

    columns as (

        select

            table_schema as "table_schema",
            table_name as "table_name",
            null as "table_comment",

            column_name as "column_name",
            ordinal_position as "column_index",
            data_type as "column_type",
            null as "column_comment"

        from information_schema.columns

    )

    select *
    from tables
    join columns using ("table_schema", "table_name")
    where "table_schema" != 'INFORMATION_SCHEMA'
    order by "column_index"

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
