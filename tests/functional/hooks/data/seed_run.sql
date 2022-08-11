
drop table if exists {schema}.on_run_hook;

create table {schema}.on_run_hook (
    "state"            TEXT, -- start|end

    "target.dbname"    TEXT,
    "target.host"      TEXT,
    "target.name"      TEXT,
    "target.schema"    TEXT,
    "target.type"      TEXT,
    "target.user"      TEXT,
    "target.pass"      TEXT,
    "target.port"      INTEGER,
    "target.threads"   INTEGER,

    "run_started_at"   TEXT,
    "invocation_id"    TEXT
);
