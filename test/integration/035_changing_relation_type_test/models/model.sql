
{{ config(materialized=var('materialized')) }}

select '{{ var("materialized") }}' as materialization
