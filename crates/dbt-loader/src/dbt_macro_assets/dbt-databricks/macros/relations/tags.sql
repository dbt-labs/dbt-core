{% macro fetch_tags(relation) -%}
  {% if relation.is_hive_metastore() %}
    {{ exceptions.raise_compiler_error("Tags are only supported for Unity Catalog") }}
  {%- endif %}
  {% call statement('list_tags', fetch_result=True) -%}
    {{ fetch_tags_sql(relation) }}
  {% endcall %}
  {% do return(load_result('list_tags').table) %}
{%- endmacro -%}

{% macro fetch_tags_sql(relation) -%}
  SELECT tag_name, tag_value
  FROM `system`.`information_schema`.`table_tags`
  WHERE catalog_name = '{{ relation.database|lower }}' 
    AND schema_name = '{{ relation.schema|lower }}'
    AND table_name = '{{ relation.identifier|lower }}'
{%- endmacro -%}

{% macro apply_tags(relation, set_tags, unset_tags=[]) -%}
  {%- if (set_tags or unset_tags) and relation.is_hive_metastore() -%}
    {{ exceptions.raise_compiler_error("Tags are only supported for Unity Catalog") }}
  {%- endif -%}
  {%- if set_tags %}
    {%- call statement('set_tags') -%}
       {{ alter_set_tags(relation, set_tags) }}
    {%- endcall -%}
  {%- endif %}
  {%- if unset_tags %}
    {%- call statement('unset_tags') -%}
       {{ alter_unset_tags(relation, unset_tags) }}
    {%- endcall -%}
  {%- endif %}
{%- endmacro -%}

{% macro alter_set_tags(relation, tags) -%}
  ALTER {{ relation.type }} {{ relation.render() }} SET TAGS (
    {% for tag in tags -%}
      '{{ tag }}' = '{{ tags[tag] }}' {%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  )
{%- endmacro -%}

{% macro alter_unset_tags(relation, tags) -%}
  ALTER {{ relation.type }} {{ relation.render() }} UNSET TAGS (
    {% for tag in tags -%}
      '{{ tag }}' {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
  )
{%- endmacro -%}
