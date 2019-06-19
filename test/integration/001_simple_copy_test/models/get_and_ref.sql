{%- do adapter.get_relation(database=target.database, schema=target.schema, identifier='materialized') -%}

select * from {{ ref('materialized') }}
