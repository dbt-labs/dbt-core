
{% macro postgres__get_catalog() -%}

  {%- call statement('catalog', fetch_result=True) -%}

    with table_owners as (

        select
            schemaname as table_schema,
            tablename as table_name,
            tableowner as table_owner

        from pg_tables

        union all

        select
            schemaname as table_schema,
            viewname as table_name,
            viewowner as table_owner

        from pg_views

    ),

    tables as (

        select

            table_schema,
            table_name,
            table_type

        from information_schema.tables

    ),

    columns as (

        select
            table_schema,
            table_name,
            null as table_comment,
            column_name,
            ordinal_position as column_index,
            data_type as column_type,
            null as column_comment

        from information_schema.columns

    )

    select *
    from tables
    join columns using (table_schema, table_name)
    join table_owners using (table_schema, table_name)

    where table_schema != 'information_schema'
      and table_schema not like 'pg_%'

    order by column_index

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
