{%- set tgt = ref('seed') -%}
{%- set got = adapter.get_relation(database=tgt.database, schema=tgt.schema, identifier=tgt.identifier) | string -%}
{% set replaced = got.replace('"', '-') %}
{% set expected = "-" + tgt.database.upper() + '-.-' + tgt.schema.upper() + '-.-' + tgt.identifier.upper() + '-' %}

with cte as (
    select '{{ replaced }}' as name
)
select * from cte where name not like '{{ expected }}'
