
{% macro presto__get_catalog() -%}
    {%- call statement('catalog', fetch_result=True) -%}
    select * from (
    {% for database in databases %}

        (
            with tables as (

                select
                    table_catalog as "table_database",
                    table_schema as "table_schema",
                    table_name as "table_name",
                    table_type as "table_type",
                    null as "table_owner"

                from {{ information_schema_name(database) }}.tables

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

                from {{ information_schema_name(database) }}.columns

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
    )
  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
