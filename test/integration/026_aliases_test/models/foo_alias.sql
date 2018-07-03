
{{
    config(
        alias='foo',
        materialized='table'
    )
}}

select '{{ this.name }}' as tablename
