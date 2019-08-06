
select
    cast(1 as int64) as test_int64,
    cast(1 as numeric) as test_numeric,
    cast(1 as float64) as test_float64,
        cast('inf' as float64) as test_float64_inf,
        cast('+inf' as float64) as test_float64_pos_inf,
        cast('-inf' as float64) as test_float64_neg_inf,
        cast('NaN' as float64) as test_float64_nan,
    cast(true as boolean) as test_boolean,
    cast('abc123' as string) as test_string,
    cast('abc123' as bytes) as test_bytes,
    cast('2019-01-01' as date) as test_date,
    cast('12:00:00' as time) as test_time,
    cast('2019-01-01 12:00:00' as timestamp) as test_timestamp,
        timestamp('2019-01-01T12:00:00+04:00') as test_timestamp_tz,
    st_geogfromgeojson('{ "type": "LineString", "coordinates": [ [1, 1], [3, 2] ] }') as test_geo,
    [
        struct(1 as val_1, 2 as val_2),
        struct(3 as val_1, 4 as val_2)
    ] as test_array,
    struct(
        cast('Fname' as string) as fname,
        cast('Lname' as string) as lname
    ) as test_struct,
    cast(null as int64) as test_null
