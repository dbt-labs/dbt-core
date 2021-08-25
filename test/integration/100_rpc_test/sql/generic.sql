
/*
    Every column returned by this query should be interpreted
    as a string. If any result is coerced into a non-string (eg.
    an int, float, bool, date, NoneType, etc) then dbt did the
    wrong thing
*/

select
    '' as str_empty_string,

    'null' as str_null,

    '1' as str_int,
    '00005' as str_int_2,
    '00' as str_int_3,

    '1.1' as str_float,
    '00001.1' as str_float_2,

    'true' as str_bool,
    'True' as str_bool_2,

    '2021-01-01' as str_date,
    '2021-01-01T12:00:00Z' as str_datetime,

    -- this is obviously not a date... but Agate used to think it was!
    -- see: https://github.com/dbt-labs/dbt/issues/2984
    '0010T00000aabbccdd' as str_obviously_not_date
