{% macro partition_by(raw_partition_by) %}
  {%- if raw_partition_by is none -%}
    {{ return('') }}
  {% endif %}

  {% set partition_by_clause %}
    partition by {{ raw_partition_by }}
  {%- endset -%}

  {{ return(partition_by_clause) }}
{%- endmacro -%}


{% macro cluster_by(raw_cluster_by) %}
  {%- if raw_cluster_by is not none -%}
  cluster by
  {% if raw_cluster_by is string -%}
    {% set raw_cluster_by = [raw_cluster_by] %}
  {%- endif -%}
  {%- for cluster in raw_cluster_by -%}
    {{ cluster }}
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}

  {% endif %}

{%- endmacro -%}

{% macro bigquery_table_options(persist_docs, temporary) %}
  {% set opts = {} %}

  {% set description = get_relation_comment(persist_docs, model) %}
  {%- if description is not none -%}
    {% do opts.update({'description': "'" ~ description ~ "'"}) %}
  {% endif %}
  {% if temporary %}
    {% do opts.update({'expiration_timestamp': 'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)'}) %}
  {% endif %}

  {% set options -%}
    OPTIONS({% for opt_key, opt_val in opts.items() %}
      {{ opt_key }}={{ opt_val }}{{ "," if not loop.last }}
    {% endfor %})
  {%- endset %}
  {% do return(options) %}
{%- endmacro -%}

{% macro bigquery__create_table_as(temporary, relation, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}

  create or replace table {{ relation }}
  {{ partition_by(raw_partition_by) }}
  {{ cluster_by(raw_cluster_by) }}
  {{ bigquery_table_options(persist_docs=raw_persist_docs, temporary=temporary) }}
  as (
    {{ sql }}
  );
{% endmacro %}


{% macro bigquery__create_view_as(relation, sql) -%}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}

  create or replace view {{ relation }}
  {{ bigquery_table_options(persist_docs=raw_persist_docs, temporary=false) }}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro bigquery__create_schema(database_name, schema_name) -%}
  {{ adapter.create_schema(database_name, schema_name) }}
{% endmacro %}

{% macro bigquery__drop_schema(database_name, schema_name) -%}
  {{ adapter.drop_schema(database_name, schema_name) }}
{% endmacro %}

{% macro bigquery__drop_relation(relation) -%}
  {% call statement('drop_relation') -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro bigquery__get_columns_in_relation(relation) -%}
  {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}


{% macro bigquery__list_schemas(database) -%}
  {% set sql %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.SCHEMATA
    where UPPER(catalog_name) like UPPER('{{ database }}')
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro empty_table() %}
    {# This is the only way I know in jinja to get an empty agate table #}
    {% do store_result('_empty_table', '', None) %}
    {{ return(load_result('_empty_table')['table']) }}
{% endmacro %}


{% macro bigquery__list_relations_without_caching(information_schema, schema) -%}
  {# In bigquery, you can't query the full information schema, you can only do so
  by schema (so 'database.schema.information_schema.tables'). But our schema
  value is case-insensitive for annoying reasons involving quoting. So you
  have figure out what schemas match the given schema first, and query them each.
  #}
  {%- set query -%}
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           when table_type = 'EXTERNAL TABLE' then 'external'
           else table_type
      end as table_type
    from {{ information_schema.replace(information_schema_view='TABLES') }}
  {%- endset -%}
  {{ return(run_query(query)) }}
{%- endmacro %}


{% macro bigquery__current_timestamp() -%}
  CURRENT_TIMESTAMP()
{%- endmacro %}
