
{% macro dbt__simple_create_table(schema, identifier, dist, sort, sql) -%}
    create table "{{ schema }}"."{{ identifier }}"
      {{ dist }} {{ sort }} as (
        {{ sql }}
    );
{%- endmacro %}

{% materialization table %}
  {%- set identifier = model['name'] -%}
  {%- set non_destructive_mode = flags.NON_DESTRUCTIVE == True -%}

  {% if non_destructive_mode -%}
    {%- if adapter.already_exists(schema, identifier) -%}
        create temporary table {{ identifier }}__dbt_tmp {{ dist }} {{ sort }} as (
          {{ sql }}
        );

        {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ schema }}.{{ identifier }} ({{ dest_cols_csv }})
        (
          select {{ dest_cols_csv }}
          from "{{ identifier }}__dbt_tmp"
        );
    {%- else -%}
        {{ dbt__simple_create_table(schema, identifier, dist, sort, sql) }}
    {%- endif -%}
  {%- elif non_destructive_mode -%}
    {{ dbt__simple_create_table(schema, identifier, dist, sort, sql) }}
  {%- else -%}
    {% set tmp_identifier = identifier + '__dbt_tmp' %}
    {{ dbt__simple_create_table(schema, tmp_identifier, dist, sort, sql) }}
  {%- endif %}
{% endmaterialization %}
