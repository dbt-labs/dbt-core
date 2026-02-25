{% macro fetch_column_tags(relation) -%}
  {% if relation.is_hive_metastore() %}
    {{ exceptions.raise_compiler_error("Column tags are only supported for Unity Catalog") }}
  {%- endif %}
  {% call statement('list_column_tags', fetch_result=True) -%}
    {{ fetch_column_tags_sql(relation) }}
  {% endcall %}
  {% do return(load_result('list_column_tags').table) %}
{%- endmacro -%}

{% macro fetch_column_tags_sql(relation) -%}
  SELECT 
    column_name,
    tag_name,
    tag_value
  FROM `system`.`information_schema`.`column_tags`
  WHERE catalog_name = '{{ relation.database|lower }}'
    AND schema_name = '{{ relation.schema|lower }}'
    AND table_name = '{{ relation.identifier|lower }}';
{%- endmacro -%}

{% macro apply_column_tags(relation, column_tags) -%}
  {% if relation.is_hive_metastore() %}
    {{ exceptions.raise_compiler_error("Column tags are only supported for Unity Catalog") }}
  {%- endif %}
  {{ log("Applying column tags to relation " ~ relation) }}
  {%- if column_tags.set_column_tags %}
    {%- for column, tags in column_tags.set_column_tags.items() -%}
      {%- call statement('main') -%}
        {{ alter_set_column_tags(relation, column, tags) }}
      {%- endcall -%}
    {%- endfor -%}
  {%- endif %}
{%- endmacro -%}

{% macro alter_set_column_tags(relation, column, tags) -%}
  {# ALTER VIEW does not support setting column tags, but ALTER TABLE works for views #}
  {%- if relation.type == 'view' -%}
    ALTER TABLE {{ relation.render() }}
  {%- else -%}
    {#- DIVERGENCE BEGIN: upstream uses relation.type.render(); we use render_type() Jinja macro instead -#}
    ALTER {{ render_type(relation.type) }} {{ relation.render() }}
    {#- DIVERGENCE END -#}
  {%- endif -%}
  ALTER COLUMN `{{ column }}`
  SET TAGS (
    {%- for tag_name, tag_value in tags.items() -%}
      '{{ tag_name }}' = '{{ tag_value }}'{%- if not loop.last %}, {% endif -%}
    {%- endfor -%}
  )
{%- endmacro -%}

{% macro column_tags_exist() %}
  {% for column_name, column in model.columns.items() %}
    {% if column is mapping and column.get('databricks_tags') %}
      {{ return(true) }}
    {% endif %}
  {% endfor %}
  {{ return(false) }}
{% endmacro %}

 