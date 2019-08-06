
select
    cast(1 as smallint) as test_smallint,
    cast(1 as int) as test_int,
    cast(1 as bigint) as test_bigint,
    cast(1 as decimal) as test_decimal,
    cast(1 as numeric(12,2)) as test_numeric,
    cast(true as boolean) as test_boolean,
    cast('abc123' as char) as test_char,
    cast('abc123' as varchar) as test_varchar,
    cast('abc123' as text) as test_text,
    cast('2019-01-01' as date) as test_date,
    cast('2019-01-01 12:00:00' as timestamp) as test_timestamp,
    cast('2019-01-01 12:00:00+04:00' as timestamptz) as test_timestamptz,
    cast(null as int) as test_null
