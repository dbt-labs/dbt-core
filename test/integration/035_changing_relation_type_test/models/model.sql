

{{ config(materialized=var('materialized'), sql_where='TRUE') }}

select '{{ var("materialized") }}' as materialization
