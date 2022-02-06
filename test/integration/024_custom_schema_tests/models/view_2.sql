
{{ config(schema='custom') }}

select * from {{ ref('view_1') }}
