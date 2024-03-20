
drop table if exists {schema}.on_model_hook;

create table {schema}.on_model_hook (
    test_state       TEXT, -- start|end
    target_dbname    TEXT,
    target_host      TEXT,
    target_name      TEXT,
    target_schema    TEXT,
    target_type      TEXT,
    target_user      TEXT,
    target_pass      TEXT,
    target_threads   INTEGER,
    run_started_at   TEXT,
    invocation_id    TEXT,
    thread_id        TEXT
);
