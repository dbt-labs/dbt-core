{% macro partition_by(raw_partition_by) %}
  {%- if raw_partition_by is none -%}
    {{ return('') }}
  {% endif %}

  {% set partition_by_clause %}
    partition by {{ raw_partition_by }}
  {%- endset -%}

  {{ return(partition_by_clause) }}
{%- endmacro -%}

{% macro bigquery__create_table_as(temporary, identifier, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}

  create or replace table `{{ schema }}`.`{{ identifier }}`
  {{ partition_by(raw_partition_by) }}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro bigquery__create_view_as(identifier, sql) -%}
  create or replace view `{{ schema }}`.`{{ identifier }}` as (
    {{ sql }}
  );
{% endmacro %}
