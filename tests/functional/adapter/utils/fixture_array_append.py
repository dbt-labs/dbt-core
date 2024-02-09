# array_append

models__array_append_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,4]) }} as array_col union all
select 2 as id, {{ array_construct([4]) }} as array_col
"""


models__array_append_actual_sql = """
select 1 as id, {{ array_append(array_construct([1,2,3]), 4) }} as array_col union all
select 2 as id, {{ array_append(array_construct([]), 4) }} as array_col
"""
