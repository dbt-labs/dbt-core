# not strictly necessary, but this reflects the integration tests currently in the 'dbt-metrics' package right now
# i'm including just the first 10 rows for more concise 'git diff'

mock_purchase_data_csv = """purchased_at,payment_type,payment_total
2021-02-14 17:52:36,maestro,2418.94
2021-02-15 04:16:50,jcb,3043.28
2021-02-15 11:30:45,solo,1505.81
2021-02-16 13:08:18,,1532.85
2021-02-17 05:41:34,americanexpress,319.91
2021-02-18 06:47:32,jcb,2143.44
2021-02-19 01:37:09,jcb,840.1
2021-02-19 03:38:49,jcb,1388.18
2021-02-19 04:22:41,jcb,2834.96
2021-02-19 13:28:50,china-unionpay,2440.98
""".strip()

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

# TODO: Add examples with versioning??
valid_time_spines_yml = """
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
