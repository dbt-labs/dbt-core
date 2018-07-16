
drop table if exists {schema}.on_run_hook;

create table {schema}.on_run_hook (
    state            STRING, -- start|end

    target_dbname    STRING,
    target_host      STRING,
    target_name      STRING,
    target_schema    STRING,
    target_type      STRING,
    target_user      STRING,
    target_pass      STRING,
    target_threads   INT64,

    run_started_at   STRING,
    invocation_id    STRING
);