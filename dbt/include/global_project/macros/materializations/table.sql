{% macro run_hooks(hooks) -%}
  {% statement %}
    {% for hook in hooks %}
      {{ hook }};
    {% endfor %}
  {% endstatement %}
{% endmacro %}

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
      {{ adapter.truncate(profile, identifier) }}
    {% elif existing_type == 'view' -%}
      {{ adapter.drop(identifier, existing_type) }}
    {%- endif %}
  {%- endif %}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% statement -%}
    {%- if existing_type is not none -%}
      {%- if non_destructive_mode -%}
        create temporary table {{ identifier }}__dbt_tmp {{ dist }} {{ sort }} as (
          {{ sql }}
        );

        {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ schema }}.{{ identifier }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from "{{ identifier }}__dbt_tmp"
        );
      {%- else -%}
        {{ dbt__simple_create_table(schema, tmp_identifier, dist, sort, sql) }}
      {%- endif -%}
    {%- else -%}
      {{ dbt__simple_create_table(schema, identifier, dist, sort, sql) }}
    {%- endif -%}
  {%- endstatement %}

  {{ run_hooks(post_hooks) }}

  -- cleanup
  {% if not non_destructive_mode -%}
    {%- if existing_type is not none -%}
      {{ adapter.drop(identifier, existing_type) }}
      {{ adapter.rename(tmp_identifier, identifier) }}
    {%- endif %}
  {%- endif %}
{% endmaterialization %}
