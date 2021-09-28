{# See here: https://github.com/dbt-labs/dbt/pull/1729 #}

select * from {{ ref('ephemeral_model') }}
