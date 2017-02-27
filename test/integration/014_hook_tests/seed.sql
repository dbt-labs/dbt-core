
drop table run_hooks_014.on_run_hook;

create table run_hooks_014.on_run_hook (
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


drop table model_hooks_014.on_model_hook;

create table model_hooks_014.on_model_hook (
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
