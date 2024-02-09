# array_construct

models__array_construct_expected_sql = """
select 1 as id, {{ array_construct([1,2,3]) }} as array_col union all
select 2 as id, {{ array_construct([]) }} as array_col
"""


models__array_construct_actual_sql = """
select 1 as id, {{ array_construct([1,2,3]) }} as array_col union all
select 2 as id, {{ array_construct([]) }} as array_col
"""
