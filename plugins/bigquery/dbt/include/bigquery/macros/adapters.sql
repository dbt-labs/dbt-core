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


{% macro bigquery__create_table_as(temporary, relation, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}

  create or replace table {{ relation }}
  {{ partition_by(raw_partition_by) }}
  {{ cluster_by(raw_cluster_by) }}
  as (
    {{ sql }}
  );
{% endmacro %}


{% macro bigquery__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro bigquery__create_schema(database_name, schema_name) -%}
  {{ adapter.create_schema(database_name, schema_name) }}
{% endmacro %}

{% macro bigquery__drop_schema(database_name, schema_name) -%}
  {{ adapter.drop_schema(database_name, schema_name) }}
{% endmacro %}

{% macro bigquery__get_columns_in_relation(relation) -%}
  {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}


{% macro bigquery__list_relations_without_caching(information_schema, schema) -%}
  {{ return(adapter.list_relations_without_caching(information_schema, schema)) }}
{% endmacro %}


{% macro bigquery__current_timestamp() -%}
  CURRENT_TIMESTAMP()
{%- endmacro %}
