# array_concat

models__array_concat_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,4,5,6]) }} as array_col union all
select 2 as id, {{ array_construct([2]) }} as array_col union all
select 3 as id, {{ array_construct([3]) }} as array_col
"""


models__array_concat_actual_sql = """
select 1 as id, {{ array_concat(array_construct([1,2,3]), array_construct([4,5,6])) }} as array_col union all
select 2 as id, {{ array_concat(array_construct([]), array_construct([2])) }} as array_col union all
select 3 as id, {{ array_concat(array_construct([3]), array_construct([])) }} as array_col
"""
