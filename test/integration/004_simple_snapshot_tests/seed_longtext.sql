create table {database}.{schema}.super_long (
    id INTEGER,
    longstring TEXT,
    updated_at TIMESTAMP WITHOUT TIME ZONE
);

insert into {database}.{schema}.super_long (id, longstring, updated_at) VALUES
(1, 'short', current_timestamp),
(2, repeat('a', 500), current_timestamp);
