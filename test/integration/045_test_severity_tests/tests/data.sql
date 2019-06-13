{{ config(severity='error' if var('strict', false) else 'warn') }}
select * from {{ ref('model') }} where email is null
