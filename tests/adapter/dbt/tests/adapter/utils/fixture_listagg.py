# listagg

seeds__data_listagg_csv = """group_col,string_text,order_col
1,a,1
1,b,2
1,c,3
2,a,2
2,1,1
2,p,3
3,g,1
3,g,2
3,g,3
"""


seeds__data_listagg_output_csv = """group_col,expected,version
1,"a_|_b_|_c",bottom_ordered
2,"1_|_a_|_p",bottom_ordered
3,"g_|_g_|_g",bottom_ordered
1,"a_|_b",bottom_ordered_limited
2,"1_|_a",bottom_ordered_limited
3,"g_|_g",bottom_ordered_limited
3,"g, g, g",comma_whitespace_unordered
3,"g",distinct_comma
3,"g,g,g",no_params
"""


models__test_listagg_sql = """
with data as (

    select * from {{ ref('data_listagg') }}

),

data_output as (

    select * from {{ ref('data_listagg_output') }}

),

calculate as (

    select
        group_col,
        {{ listagg('string_text', "'_|_'", "order by order_col") }} as actual,
        'bottom_ordered' as version
    from data
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text', "'_|_'", "order by order_col", 2) }} as actual,
        'bottom_ordered_limited' as version
    from data
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text', "', '") }} as actual,
        'comma_whitespace_unordered' as version
    from data
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('DISTINCT string_text', "','") }} as actual,
        'distinct_comma' as version
    from data
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text') }} as actual,
        'no_params' as version
    from data
    where group_col = 3
    group by group_col

)

select
    calculate.actual,
    data_output.expected
from calculate
left join data_output
on calculate.group_col = data_output.group_col
and calculate.version = data_output.version
"""


models__test_listagg_yml = """
version: 2
models:
  - name: test_listagg
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
