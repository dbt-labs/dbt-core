{%- do adapter.get_relation(database=target.database, schema=target.schema, identifier='MATERIALIZED') -%}

select * from {{ ref('MATERIALIZED') }}
