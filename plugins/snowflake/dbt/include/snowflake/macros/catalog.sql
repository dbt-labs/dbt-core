
{% macro snowflake__get_catalog() -%}

    {%- call statement('catalog', fetch_result=True) -%}
    {% for database in databases %}

        (
            with tables as (

                select
                    table_catalog as "table_database",
                    table_schema as "table_schema",
                    table_name as "table_name",
                    table_type as "table_type",

                    -- note: this is the _role_ that owns the table
                    table_owner as "table_owner",

                    'Clustering Key' as "stats:clustering_key:label",
                    clustering_key as "stats:clustering_key:value",
                    'The key used to cluster this table' as "stats:clustering_key:description",
                    (clustering_key is not null) as "stats:clustering_key:include",

                    'Row Count' as "stats:row_count:label",
                    row_count as "stats:row_count:value",
                    'An approximate count of rows in this table' as "stats:row_count:description",
                    (row_count is not null) as "stats:row_count:include",

                    'Approximate Size' as "stats:bytes:label",
                    bytes as "stats:bytes:value",
                    'Approximate size of the table as reported by Snowflake' as "stats:bytes:description",
                    (bytes is not null) as "stats:bytes:include"

                from {{ adapter.quote_as_configured(database, "database") }}.information_schema.tables

            ),

            columns as (

                select
                    table_catalog as "table_database",
                    table_schema as "table_schema",
                    table_name as "table_name",
                    null as "table_comment",

                    column_name as "column_name",
                    ordinal_position as "column_index",
                    data_type as "column_type",
                    null as "column_comment"

                from {{ adapter.quote_as_configured(database, "database") }}.information_schema.columns

            )

            select *
            from tables
            join columns using ("table_database", "table_schema", "table_name")
            where "table_schema" != 'INFORMATION_SCHEMA'
              and "table_database" = {{ adapter.quote_as_configured(database, "database").replace('"', "'") }}
            order by "column_index"
        )
        {% if not loop.last %} union all {% endif %}

    {% endfor %}
  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
