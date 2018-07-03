
{{
    config(
        alias='foo',
        materialized='table'
    )
}}

select {{ string_literal(this.name) }} as tablename
