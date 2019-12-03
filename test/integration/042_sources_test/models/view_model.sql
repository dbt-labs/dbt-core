{# See here: https://github.com/fishtown-analytics/dbt/pull/1729 #}

select * from {{ ref('ephemeral_model') }}
