orders_sql = """
select 1 as order_id, 1 as customer_id, 10 as amount, current_date as ordered_at
"""

customers_sql = """
select 1 as customer_id, 'west' as region
"""

metricflow_time_spine_sql = """
select cast(d as date) as date_day
from generate_series('2020-01-01'::date, '2030-01-01'::date, '1 day') d
"""

semantic_layer_yml = """
models:
  - name: metricflow_time_spine
    time_spine:
      standard_granularity_column: date_day
    columns:
      - name: date_day
        granularity: day

semantic_models:
  - name: orders
    model: ref('orders')
    defaults:
      agg_time_dimension: ordered_at
    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: ordered_at
        type: time
        type_params:
          time_granularity: day
    measures:
      - name: revenue
        agg: sum
        expr: amount

  - name: customers
    model: ref('customers')
    entities:
      - name: customer
        type: primary
        expr: customer_id
    dimensions:
      - name: region
        type: categorical

metrics:
  - name: revenue
    label: Revenue
    type: simple
    type_params:
      measure: revenue

saved_queries:
  - name: revenue_by_region
    query_params:
      metrics:
        - revenue
      group_by:
        - "Dimension('customer__region')"
"""

semantic_query_model_sql = """
select * from (
    {{ metric_query(
        metrics='revenue',
        group_by=['customer__region'],
        where="{{ Dimension('customer__region') }} = 'west'",
        limit=10,
    ) }}
) as revenue_by_region
"""

saved_query_model_sql = """
select * from (
    {{ saved_query('revenue_by_region') }}
) as revenue_by_region
"""

semantic_query_analysis_sql = """
select * from (
    {{ metric_query(metrics=['revenue']) }}
) as total_revenue
"""

semantic_query_singular_test_sql = """
select * from (
    {{ metric_query(metrics=['revenue']) }}
) as total_revenue
"""
