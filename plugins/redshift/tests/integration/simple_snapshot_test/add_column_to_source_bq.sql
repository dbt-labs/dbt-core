
create or replace table {schema}.seed as (

    select *,
        [
            struct(
                1 as field_1,
                2 as field_2
            ),
            struct(
                3 as field_1,
                4 as field_2
            )
        ] as repeated_nested_field,

        struct(
            1 as field_1,
            2 as field_2
        ) as nested_field,

        [
            1,
            2
        ] as repeated_field

    from {schema}.seed

);

create or replace table {schema}.snapshot_expected as (

    select *,
        [
            struct(
                1 as field_1,
                2 as field_2
            ),
            struct(
                3 as field_1,
                4 as field_2
            )
        ] as repeated_nested_field,

        struct(
            1 as field_1,
            2 as field_2
        ) as nested_field,

        [
            1,
            2
        ] as repeated_field

    from {schema}.snapshot_expected

);
