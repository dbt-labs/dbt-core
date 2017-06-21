{% macro dbt__simple_create_table(schema, identifier, dist, sort, sql) -%}
    create table "{{ schema }}"."{{ identifier }}"
      {{ dist }} {{ sort }} as (
        {{ sql }}
    );
{%- endmacro %}

{% materialization table %}
  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  -- setup
  {% if non_destructive_mode -%}
    {% if existing_type == 'table' -%}
      {{ adapter.truncate(identifier) }}
    {% elif existing_type == 'view' -%}
      {{ adapter.drop(identifier, existing_type) }}
    {%- endif %}
  {%- endif %}

  {% for hook in pre_hooks %}
    {% statement %}
      {{ hook }};
    {% endstatement %}
  {% endfor %}

  -- build model
  {% statement -%}
    {%- if non_destructive_mode -%}
      {%- if adapter.already_exists(schema, identifier) -%}
        create temporary table {{ tmp_identifier }} {{ dist }} {{ sort }} as (
          {{ sql }}
        );

        {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ schema }}.{{ identifier }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from "{{ identifier }}__dbt_tmp"
        );
      {%- else -%}
        {{ dbt__simple_create_table(schema, identifier, dist, sort, sql) }}
      {%- endif -%}
    {%- else -%}
      {{ dbt__simple_create_table(schema, tmp_identifier, dist, sort, sql) }}
    {%- endif -%}
  {%- endstatement %}

  {% for hook in post_hooks %}
    {% statement %}
      {{ hook }};
    {% endstatement %}
  {% endfor%}

  -- cleanup
  {% if non_destructive_mode -%}
    -- noop
  {%- else -%}
    {%- if existing_type is not none -%}
      {{ adapter.drop(identifier, existing_type) }}
    {%- endif %}

    {{ adapter.rename(tmp_identifier, identifier) }}
  {%- endif %}
{% endmaterialization %}
