simple_metricflow_time_spine_sql = """
SELECT to_date('02/20/2023', 'mm/dd/yyyy') as date_day
"""

models_people_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 2 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at
union all
select 3 as id, 'Callum' as first_name, 'McCann' as last_name, 'emerald' as favorite_color, true as loves_dbt, 0 as tenure, current_timestamp as created_at
"""

groups_yml = """
version: 2

groups:
  - name: some_group
    owner:
      email: me@gmail.com
  - name: some_other_group
    owner:
      email: me@gmail.com
"""

models_people_metrics_yml = """
version: 2

metrics:
  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    type: simple
    type_params:
      measure: people
    meta:
        my_meta: 'testing'
"""

disabled_models_people_metrics_yml = """
version: 2

metrics:
  - name: number_of_people
    config:
      enabled: false
      group: some_group
    label: "Number of people"
    description: Total count of people
    type: simple
    type_params:
      measure: people
    meta:
        my_meta: 'testing'
"""

semantic_model_people_yml = """
version: 2

semantic_models:
  - name: semantic_people
    label: "Semantic People"
    model: ref('people')
    dimensions:
      - name: favorite_color
        label: "Favorite Color"
        type: categorical
      - name: created_at
        label: "Created At"
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: years_tenure
        label: "Years Tenure"
        agg: SUM
        expr: tenure
      - name: people
        label: "People"
        agg: count
        expr: id
    entities:
      - name: id
        label: "Primary ID"
        type: primary
    defaults:
      agg_time_dimension: created_at
"""

semantic_model_descriptions = """
{% docs semantic_model_description %} foo {% enddocs %}
{% docs dimension_description %} bar {% enddocs %}
{% docs measure_description %} baz {% enddocs %}
{% docs entity_description %} qux {% enddocs %}
"""

semantic_model_people_yml_with_docs = """
version: 2

semantic_models:
  - name: semantic_people
    model: ref('people')
    description: "{{ doc('semantic_model_description') }}"
    dimensions:
      - name: favorite_color
        type: categorical
        description: "{{ doc('dimension_description') }}"
      - name: created_at
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: years_tenure
        agg: SUM
        expr: tenure
        description: "{{ doc('measure_description') }}"
      - name: people
        agg: count
        expr: id
    entities:
      - name: id
        description: "{{ doc('entity_description') }}"
        type: primary
    defaults:
      agg_time_dimension: created_at
"""

enabled_semantic_model_people_yml = """
version: 2

semantic_models:
  - name: semantic_people
    label: "Semantic People"
    model: ref('people')
    config:
      enabled: true
      group: some_group
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

disabled_semantic_model_people_yml = """
version: 2

semantic_models:
  - name: semantic_people
    label: "Semantic People"
    model: ref('people')
    config:
      enabled: false
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

schema_yml = """models:
  - name: fct_revenue
    description: This is the model fct_revenue. It should be able to use doc blocks

semantic_models:
  - name: revenue
    description: This is the revenue semantic model. It should be able to use doc blocks
    model: ref('fct_revenue')

    defaults:
      agg_time_dimension: ds

    measures:
      - name: txn_revenue
        expr: revenue
        agg: sum
        agg_time_dimension: ds
        create_metric: true
      - name: sum_of_things
        expr: 2
        agg: sum
        agg_time_dimension: ds
      - name: has_revenue
        expr: true
        agg: sum_boolean
        agg_time_dimension: ds
      - name: discrete_order_value_p99
        expr: order_total
        agg: percentile
        agg_time_dimension: ds
        agg_params:
          percentile: 0.99
          use_discrete_percentile: True
          use_approximate_percentile: False
      - name: test_agg_params_optional_are_empty
        expr: order_total
        agg: percentile
        agg_time_dimension: ds
        agg_params:
          percentile: 0.99
      - name: test_non_additive
        expr: txn_revenue
        agg: sum
        non_additive_dimension:
          name: ds
          window_choice: max

    dimensions:
      - name: ds
        type: time
        expr: created_at
        type_params:
          time_granularity: day

    entities:
      - name: user
        type: foreign
        expr: user_id
      - name: id
        type: primary

metrics:
  - name: simple_metric
    label: Simple Metric
    type: simple
    type_params:
      measure: sum_of_things
"""

schema_without_semantic_model_yml = """models:
  - name: fct_revenue
    description: This is the model fct_revenue. It should be able to use doc blocks
"""

fct_revenue_sql = """select
  1 as id,
  10 as user_id,
  1000 as revenue,
  current_timestamp as created_at"""

metricflow_time_spine_sql = """
with days as (
    {{dbt_utils.date_spine('day'
    , "to_date('01/01/2000','mm/dd/yyyy')"
    , "to_date('01/01/2027','mm/dd/yyyy')"
    )
    }}
),

final as (
    select cast(date_day as date) as date_day
    from days
)

select *
from final
"""
