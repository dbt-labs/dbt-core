# except

seeds__data_except_a_csv = """id
1
2
3
"""

seeds__data_except_b_csv = """id
3
4
5
"""

seeds__data_except_a_minus_b_csv = """id
1
2
"""

seeds__data_except_b_minus_a_csv = """id
4
5
"""

models__data_except_empty_sql = """
select * from {{ ref('data_except_a') }}
where 0=1
"""

models__test_except_a_minus_b_sql = """
select * from {{ ref('data_except_a') }}
{{ except() }}
select * from {{ ref('data_except_b') }}
"""

models__test_except_b_minus_a_sql = """
select * from {{ ref('data_except_b') }}
{{ except() }}
select * from {{ ref('data_except_a') }}
"""

models__test_except_a_minus_a_sql = """
select * from {{ ref('data_except_a') }}
{{ except() }}
select * from {{ ref('data_except_a') }}
"""

models__test_except_a_minus_empty_sql = """
select * from {{ ref('data_except_a') }}
{{ except() }}
select * from {{ ref('data_except_empty') }}
"""

models__test_except_empty_minus_a_sql = """
select * from {{ ref('data_except_empty') }}
{{ except() }}
select * from {{ ref('data_except_a') }}
"""

models__test_except_empty_minus_empty_sql = """
select * from {{ ref('data_except_empty') }}
{{ except() }}
select * from {{ ref('data_except_empty') }}
"""
