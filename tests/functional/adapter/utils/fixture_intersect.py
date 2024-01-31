# intersect

seeds__data_intersect_a_csv = """id
1
2
3
"""

seeds__data_intersect_b_csv = """id
3
4
5
"""

seeds__data_intersect_a_overlap_b_csv = """id
3
"""

models__data_intersect_empty_sql = """
select * from {{ ref('data_intersect_a') }}
where 0=1
"""

models__test_intersect_a_overlap_b_sql = """
select * from {{ ref('data_intersect_a') }}
{{ intersect() }}
select * from {{ ref('data_intersect_b') }}
"""

models__test_intersect_b_overlap_a_sql = """
select * from {{ ref('data_intersect_b') }}
{{ intersect() }}
select * from {{ ref('data_intersect_a') }}
"""

models__test_intersect_a_overlap_a_sql = """
select * from {{ ref('data_intersect_a') }}
{{ intersect() }}
select * from {{ ref('data_intersect_a') }}
"""

models__test_intersect_a_overlap_empty_sql = """
select * from {{ ref('data_intersect_a') }}
{{ intersect() }}
select * from {{ ref('data_intersect_empty') }}
"""

models__test_intersect_empty_overlap_a_sql = """
select * from {{ ref('data_intersect_empty') }}
{{ intersect() }}
select * from {{ ref('data_intersect_a') }}
"""

models__test_intersect_empty_overlap_empty_sql = """
select * from {{ ref('data_intersect_empty') }}
{{ intersect() }}
select * from {{ ref('data_intersect_empty') }}
"""
