models_people_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 2 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at
union all
select 3 as id, 'Callum' as first_name, 'McCann' as last_name, 'emerald' as favorite_color, true as loves_dbt, 0 as tenure, current_timestamp as created_at
"""

semantic_model_people_yml = """
version: 2

semantic_models:
  - name: semantic_people
    model: ref('people')
    dimensions:
      - name: favorite_color
        type: categorical
      - name: created_at
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: years_tenure
        agg: SUM
        expr: tenure
      - name: people
        agg: count
        expr: id
    entities:
      - name: id
        type: primary
    defaults:
      agg_time_dimension: created_at
"""

metricflow_time_spine_sql = """
SELECT to_date('02/20/2023, 'mm/dd/yyyy') as date_day
"""

metricflow_time_spine_second_sql = """
SELECT to_datetime('02/20/2023, 'mm/dd/yyyy hh:mm:ss') as ts_second
"""

valid_time_spines_yml = """
version: 2

models:
  - name: metricflow_time_spine_second
    time_spine:
      standard_granularity_column: ts_second
    columns:
      - name: ts_second
        granularity: second
  - name: metricflow_time_spine
    time_spine:
      standard_granularity_column: date_day
    columns:
      - name: date_day
        granularity: day
"""

missing_time_spine_yml = """
models:
  - name: metricflow_time_spine
    columns:
      - name: ts_second
        granularity: second
"""

time_spine_missing_granularity_yml = """
models:
  - name: metricflow_time_spine_second
    time_spine:
      standard_granularity_column: ts_second
    columns:
      - name: ts_second
"""

time_spine_missing_column_yml = """
models:
  - name: metricflow_time_spine_second
    time_spine:
      standard_granularity_column: ts_second
    columns:
      - name: date_day
"""
