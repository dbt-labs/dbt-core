
{{
    config(
        materialized='table'
    )
}}

select

    -- compile-time variables
    '{{ this }}'        as "this",
    '{{ this.name }}'   as "this.name",
    '{{ this.schema }}' as "this.schema",
    '{{ this.table }}'  as "this.table",

    '{{ target.dbname }}'  as "target.dbname",
    '{{ target.host }}'    as "target.host",
    '{{ target.name }}'    as "target.name",
    '{{ target.schema }}'  as "target.schema",
    '{{ target.type }}'    as "target.type",
    '{{ target.user }}'    as "target.user",
    '{{ target.get("pass", "") }}'    as "target.pass", -- not actually included, here to test that it is _not_ present!
    {{ target.port }}      as "target.port",
    {{ target.threads }}   as "target.threads",

    -- runtime variables
    '{{ run_started_at }}' as run_started_at,
    '{{ invocation_id }}'  as invocation_id,

    '{{ env_var("DBT_TEST_013_ENV_VAR") }}' as env_var,
    'secret_variable' as env_var_secret, -- make sure the value itself is scrubbed from the logs
    '{{ env_var("DBT_TEST_013_NOT_SECRET") }}' as env_var_not_secret
