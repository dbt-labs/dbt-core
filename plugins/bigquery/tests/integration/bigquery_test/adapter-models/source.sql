
with nested_base as (
  select
    struct(
      'a' as field_a,
      'b' as field_b
     ) as repeated_nested

   union all

   select
     struct(
      'a' as field_a,
      'b' as field_b
     ) as repeated_nested
),

nested as (

  select
    array_agg(repeated_nested) as repeated_column

  from nested_base

),

base as (

  select
      1 as field_1,
      2 as field_2,
      3 as field_3,

      struct(
          4 as field_4,
          5 as field_5
      ) as nested_field
)

select *
from base, nested
