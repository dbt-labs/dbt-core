
{{
    config(
        materialized = 'ephemeral',
        tags = ['base']
    )
}}

select * from {{ this.schema }}.seed
