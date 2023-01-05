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
select 1 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Callum' as first_name, 'McCann' as last_name, 'emerald' as favorite_color, true as loves_dbt, 0 as tenure, current_timestamp as created_at
"""

basic_metrics_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    model: "ref('people')"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day, week, month]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

  - name: average_tenure
    label: "Average tenure"
    description: "The average tenure per person"
    calculation_method: derived
    expression: "{{metric('collective_tenure')}} / {{metric('number_of_people')}} "
    timestamp: created_at
    time_grains: [day, week, month]

  - name: average_tenure_plus_one
    label: "Average tenure"
    description: "The average tenure per person"
    calculation_method: derived
    expression: "{{metric('average_tenure')}} + 1 "
    timestamp: created_at
    time_grains: [day, week, month]
"""

models_people_metrics_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    model: "ref('people')"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

  - name: collective_window
    label: "Collective window"
    description: Testing window
    model: "ref('people')"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    window:
      count: 14
      period: day
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""

invalid_models_people_metrics_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref(people)"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    model: "ref(people)"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""

invalid_metrics_missing_model_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""

invalid_metrics_missing_expression_yml = """
version: 2
metrics:
  - name: number_of_people
    label: "Number of people"
    model: "ref(people)"
    description: Total count of people
    calculation_method: count
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'
"""

names_with_spaces_metrics_yml = """
version: 2

metrics:

  - name: number of people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""

names_with_special_chars_metrics_yml = """
version: 2

metrics:

  - name: number_of_people!
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""


names_with_leading_numeric_metrics_yml = """
version: 2

metrics:

  - name: 1_number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""

long_name_metrics_yml = """
version: 2

metrics:

  - name: this_name_is_going_to_contain_more_than_250_characters_but_be_otherwise_acceptable_and_then_will_throw_an_error_which_I_expect_to_happen_and_repeat_this_name_is_going_to_contain_more_than_250_characters_but_be_otherwise_acceptable_and_then_will_throw_an_error_which_I_expect_to_happen
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

"""

downstream_model_sql = """
-- this model will depend on these three metrics
{% set some_metrics = [
    metric('count_orders'),
    metric('sum_order_revenue'),
    metric('average_order_value')
] %}

/*
{% if not execute %}

    -- the only properties available to us at 'parse' time are:
    --      'metric_name'
    --      'package_name' (None if same package)

    {% set metric_names = [] %}
    {% for m in some_metrics %}
        {% do metric_names.append(m.metric_name) %}
    {% endfor %}

    -- this config does nothing, but it lets us check these values below
    {{ config(metric_names = metric_names) }}

{% else %}

    -- these are the properties available to us at 'execution' time

    {% for m in some_metrics %}
        name: {{ m.name }}
        label: {{ m.label }}
        calculation_method: {{ m.calculation_method }}
        expression: {{ m.expression }}
        timestamp: {{ m.timestamp }}
        time_grains: {{ m.time_grains }}
        dimensions: {{ m.dimensions }}
        filters: {{ m.filters }}
        window: {{ m.window }}
    {% endfor %}

{% endif %}

select 1 as id
"""

invalid_derived_metric_contains_model_yml = """
version: 2
metrics:
    - name: count_orders
      label: Count orders
      model: ref('mock_purchase_data')

      calculation_method: count
      expression: "*"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: sum_order_revenue
      label: Total order revenue
      model: ref('mock_purchase_data')

      calculation_method: sum
      expression: "payment_total"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: average_order_value
      label: Average Order Value

      calculation_method: derived
      expression:  "{{metric('sum_order_revenue')}} / {{metric('count_orders')}} "
      model: ref('mock_purchase_data')
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type
"""

derived_metric_yml = """
version: 2
metrics:
    - name: count_orders
      label: Count orders
      model: ref('mock_purchase_data')

      calculation_method: count
      expression: "*"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: sum_order_revenue
      label: Total order revenue
      model: ref('mock_purchase_data')

      calculation_method: sum
      expression: "payment_total"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: average_order_value
      label: Average Order Value

      calculation_method: derived
      expression:  "{{metric('sum_order_revenue')}} / {{metric('count_orders')}} "
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type
"""

derived_metric_old_attr_names_yml = """
version: 2
metrics:
    - name: count_orders
      label: Count orders
      model: ref('mock_purchase_data')

      type: count
      sql: "*"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: sum_order_revenue
      label: Total order revenue
      model: ref('mock_purchase_data')

      type: sum
      sql: "payment_total"
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type

    - name: average_order_value
      label: Average Order Value

      type: expression
      sql:  "{{metric('sum_order_revenue')}} / {{metric('count_orders')}} "
      timestamp: purchased_at
      time_grains: [day, week, month, quarter, year]

      dimensions:
        - payment_type
"""

disabled_metric_level_schema_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    config:
      enabled: False
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    model: "ref('people')"
    calculation_method: sum
    expression: "*"
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""

enabled_metric_level_schema_yml = """
version: 2

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    config:
      enabled: True
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - name: collective_tenure
    label: "Collective tenure"
    description: Total number of years of team experience
    model: "ref('people')"
    calculation_method: sum
    expression: "*"
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: 'is'
        value: 'true'

"""

models_people_metrics_sql = """
-- this model will depend on these two metrics
{% set some_metrics = [
    metric('number_of_people'),
    metric('collective_tenure')
] %}

/*
{% if not execute %}

    -- the only properties available to us at 'parse' time are:
    --      'metric_name'
    --      'package_name' (None if same package)

    {% set metric_names = [] %}
    {% for m in some_metrics %}
        {% do metric_names.append(m.metric_name) %}
    {% endfor %}

    -- this config does nothing, but it lets us check these values below
    {{ config(metric_names = metric_names) }}

{% else %}

    -- these are the properties available to us at 'execution' time

    {% for m in some_metrics %}
        name: {{ m.name }}
        label: {{ m.label }}
        calculation_method: {{ m.calculation_method }}
        expression: {{ m.expression }}
        timestamp: {{ m.timestamp }}
        time_grains: {{ m.time_grains }}
        dimensions: {{ m.dimensions }}
        filters: {{ m.filters }}
        window: {{ m.window }}
    {% endfor %}

{% endif %}

select 1 as id
"""

metrics_1_yml = """
version: 2

metrics:
  - name: some_metric
    label: Some Metric
    model: ref('model_a')

    calculation_method: count
    expression: id

    timestamp: ts
    time_grains: [day]
"""

metrics_2_yml = """
version: 2

metrics:
  - name: some_metric
    label: Some Metric
    model: ref('model_a')

    calculation_method: count
    expression: user_id

    timestamp: ts
    time_grains: [day]
"""

model_a_sql = """
select 1 as fun
"""

model_b_sql = """
-- {{ metric('some_metric') }}

{% if execute %}
  {% set model_ref_node = graph.nodes.values() | selectattr('name', 'equalto', 'model_a') | first %}
  {% set relation = api.Relation.create(
      database = model_ref_node.database,
      schema = model_ref_node.schema,
      identifier = model_ref_node.alias
  )
  %}
{% else %}
  {% set relation = "" %}
{% endif %}

-- this one is a real ref
select * from {{ ref('model_a') }}
union all
-- this one is synthesized via 'graph' var
select * from {{ relation }}
"""

invalid_config_metric_yml = """
version: 2

metrics:
  - name: number_of_people
    label: "Number of people"
    config:
        enabled: True and False
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'
"""

metric_without_timestamp_or_timegrains_yml = """
version: 2

metrics:
  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    calculation_method: count
    expression: "*"
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'
"""

invalid_metric_without_timestamp_with_time_grains_yml = """
version: 2

metrics:
  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    time_grains: [day, week, month]
    calculation_method: count
    expression: "*"
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'
"""

invalid_metric_without_timestamp_with_window_yml = """
version: 2

metrics:
  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    model: "ref('people')"
    window:
      count: 14
      period: day
    calculation_method: count
    expression: "*"
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'
"""
