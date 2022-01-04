{# If our dependency source didn't exist, this would be an errror #}
select * from {{ source('seed_source', 'seed') }}
