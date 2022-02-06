{{
  config(
    materialized = "table"
  )
}}
-- ensure that dbt_utils' relation check will work
{% set relation = ref('seed') %}
{%- if not (relation is mapping and relation.get('metadata', {}).get('type', '').endswith('Relation')) -%}
    {%- do exceptions.raise_compiler_error("Macro " ~ macro ~ " expected a Relation but received the value: " ~ relation) -%}
{%- endif -%}
-- this is a unicode character: å
select * from {{ relation }}
