{% macro clickhouse__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    select
      '' as table_database,
      columns.database as table_schema,
      columns.table as table_name,
      if(tables.engine not in ('MaterializedView', 'View'), 'table', 'view') as table_type,
      nullIf(tables.comment, '') as table_comment,
      columns.name as column_name,
      columns.position as column_index,
      columns.type as column_type,
      nullIf(columns.comment, '') as column_comment,
      null as table_owner
    from system.columns as columns
    join system.tables as tables on tables.database = columns.database and tables.name = columns.table
    where database != 'system' and
    (
    {%- for schema in schemas -%}
      columns.database = '{{ schema }}'
      {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    )
    order by columns.database, columns.table, columns.position
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
