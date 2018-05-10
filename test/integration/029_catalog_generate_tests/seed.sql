create table {schema}.seed (
	id INTEGER,
	first_name VARCHAR(11),
	email VARCHAR(31),
	ip_address VARCHAR(15),
	updated_at TIMESTAMP WITHOUT TIME ZONE
);


INSERT INTO {schema}.seed
    ("id","first_name","email","ip_address","updated_at")
VALUES
    (1,'Larry','lking0@miitbeian.gov.cn','69.135.206.194','2008-09-12 19:08:31');

create table {schema}.seed_config_expected_1 as (

    select *, 'default'::text as c1, 'default'::text as c2, 'was true'::text as some_bool from {schema}.seed

);


create table {schema}.seed_summary (
    year timestamp without time zone,
    count bigint
);

INSERT INTO {schema}.seed_summary
    ("year","count")
VALUES
    ('2008-01-01 00:00:00',6);

