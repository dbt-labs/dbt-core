
-- - get_catalog
-- - list_relations_without_caching
-- - get_columns_in_relation

{% macro presto_ilike(column, value) -%}
	regexp_like({{ column }}, '(?i)\A{{ value }}\Z')
{%- endmacro %}


{% macro presto__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          case when regexp_like(data_type, 'varchar\(\d+\)') then 'varchar'
               else data_type
          end as data_type,
          case when regexp_like(data_type, 'varchar\(\d+\)') then
                  from_base(regexp_extract(data_type, 'varchar\((\d+)\)', 1), 10)
               else NULL
          end as character_maximum_length,
          NULL as numeric_precision,
          NULL as numeric_scale

      from
      {{ information_schema_name(relation.database) }}.columns

      where {{ presto_ilike('table_name', relation.identifier) }}
        {% if relation.schema %}
        and {{ presto_ilike('table_schema', relation.schema) }}
        {% endif %}
        {% if relation.database %}
        and {{ presto_ilike('table_catalog', relation.database) }}
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro presto__list_relations_without_caching(database, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from {{ information_schema_name(database) }}.tables
    where {{ presto_ilike('table_schema', schema) }}
      and {{ presto_ilike('table_catalog', database) }}
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro presto__reset_csv_table(model, full_refresh, old_relation) %}
    {{ adapter.drop_relation(old_relation) }}
    {{ return(create_csv_table(model)) }}
{% endmacro %}


{% macro presto__create_table_as(temporary, relation, sql) -%}
  create table
    {{ relation }}
  as (
    {{ sql }}
  );
{% endmacro %}


{% macro presto__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}


{% macro presto__drop_schema(database_name, schema_name) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{database_name}}.{{schema_name}}
  {% endcall %}
{% endmacro %}


{% macro presto__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro presto__load_csv_rows(model) %}
  {{ return(basic_load_csv_rows(model, 1000)) }}
{% endmacro %}
