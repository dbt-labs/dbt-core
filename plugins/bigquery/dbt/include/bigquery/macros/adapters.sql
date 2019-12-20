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

{% macro bigquery_table_options(persist_docs, temporary, kms_key_name, labels) %}
  {% set opts = {} -%}

  {%- set description = get_relation_comment(persist_docs, model) -%}
  {%- if description is not none -%}
    {%- do opts.update({'description': "'" ~ description ~ "'"}) -%}
  {%- endif -%}
  {%- if temporary -%}
    {% do opts.update({'expiration_timestamp': 'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)'}) %}
  {%- endif -%}
  {%- if kms_key_name -%}
    {%- do opts.update({'kms_key_name': "'" ~ kms_key_name ~ "'"}) -%}
  {%- endif -%}
  {%- if labels -%}
    {%- set label_list = [] -%}
    {%- for label, value in labels.items() -%}
      {%- do label_list.append((label, value)) -%}
    {%- endfor -%}
    {%- do opts.update({'labels': label_list}) -%}
  {%- endif -%}

  {% set options -%}
    OPTIONS({% for opt_key, opt_val in opts.items() %}
      {{ opt_key }}={{ opt_val }}{{ "," if not loop.last }}
    {% endfor %})
  {%- endset %}
  {%- do return(options) -%}
{%- endmacro -%}

{% macro bigquery__create_table_as(temporary, relation, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}
  {%- set raw_kms_key_name = config.get('kms_key_name', none) -%}
  {%- set raw_labels = config.get('labels', []) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace table {{ relation }}
  {{ partition_by(raw_partition_by) }}
  {{ cluster_by(raw_cluster_by) }}
  {{ bigquery_table_options(
      persist_docs=raw_persist_docs, temporary=temporary, kms_key_name=raw_kms_key_name,
      labels=raw_labels) }}
  as (
    {{ sql }}
  );
{%- endmacro -%}


{% macro bigquery__create_view_as(relation, sql) -%}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}
  {%- set raw_labels = config.get('labels', []) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation }}
  {{ bigquery_table_options(persist_docs=raw_persist_docs, temporary=false, labels=raw_labels) }}
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


{% macro bigquery__list_relations_without_caching(information_schema, schema) -%}
  {{ return(adapter.list_relations_without_caching(information_schema, schema)) }}
{%- endmacro %}


{% macro bigquery__current_timestamp() -%}
  CURRENT_TIMESTAMP()
{%- endmacro %}


{% macro bigquery__snapshot_string_as_time(timestamp) -%}
    {%- set result = 'TIMESTAMP("' ~ timestamp ~ '")' -%}
    {{ return(result) }}
{%- endmacro %}


{% macro bigquery__list_schemas(database) -%}
  {{ return(adapter.list_schemas()) }}
{% endmacro %}


{% macro bigquery__check_schema_exists(information_schema, schema) %}
  {{ return(adapter.check_schema_exists(information_schema.database, schema)) }}
{% endmacro %}
