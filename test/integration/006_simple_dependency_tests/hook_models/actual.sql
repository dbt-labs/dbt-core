select * from {{ var('test_create_table') }}
union all
select * from {{ var('test_create_second_table') }}
