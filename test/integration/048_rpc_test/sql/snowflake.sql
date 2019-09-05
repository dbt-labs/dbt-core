
select
    cast(1 as number(12, 2)) as test_number,
    cast(1 as int) as test_int,
    cast(1 as float) as test_float,
    cast('abc123' as varchar) as test_varchar,
    cast('abc123' as char(6)) as test_char,
    cast('2019-01-01' as date) as test_date,
    cast('2019-01-01 12:00:00' as datetime) as test_datetime,
    cast('12:00:00' as time) as test_time,
    cast('2019-01-01 12:00:00' as timestamp_ltz) as test_timestamp_ltz,
    cast('2019-01-01 12:00:00' as timestamp_ntz) as test_timestamp_ntz,
    cast('2019-01-01 12:00:00' as timestamp_tz) as test_timestamp_tz,
    cast(parse_json('{"a": 1, "b": 2}') as variant) as test_variant,
    cast(parse_json('{"a": 1, "b": 2}') as object) as test_object,
    cast(parse_json('[{"a": 1, "b": 2}]') as array) as test_array

    -- This fails inside of Agate on Py2
    --cast('C0FF33' as binary) as test_binary,
