{{
    config(
        materialized='table'
    )
}}

select
    1 as field_1,
    2 as field_2,
    3 as field_3,

    struct(
        5 as field_5,
        6 as field_6
    ) as nested_field
