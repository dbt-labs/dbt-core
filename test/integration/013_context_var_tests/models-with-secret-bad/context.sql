
{{
    config(
        materialized='table'
    )
}}

select

    '{{ env_var("DBT_TEST_013_ENV_VAR") }}' as env_var,
    '{{ env_var("DBT_ENV_SECRET_013_SECRET") }}' as env_var_secret, -- this should raise an error!
    '{{ env_var("DBT_TEST_013_NOT_SECRET") }}' as env_var_not_secret
