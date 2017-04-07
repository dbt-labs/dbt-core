
{{
    config({
        'materialized': 'table',
        'post-hook': [ '{{ hook() }}' ]
    })
}}

select 1 as id
