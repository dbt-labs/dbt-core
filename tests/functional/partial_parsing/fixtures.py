local_dependency__dbt_project_yml = """

name: 'local_dep'
version: '1.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

require-dbt-version: '>=0.1.0'

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
    - "target"
    - "dbt_packages"


seeds:
  quote_columns: False

"""

local_dependency__models__schema_yml = """
version: 2
sources:
  - name: seed_source
    schema: "{{ var('schema_override', target.schema) }}"
    tables:
      - name: "seed"
        columns:
          - name: id
            tests:
              - unique

"""

local_dependency__models__model_to_import_sql = """
select * from {{ ref('seed') }}

"""

local_dependency__macros__dep_macro_sql = """
{% macro some_overridden_macro() -%}
100
{%- endmacro %}

"""

local_dependency__seeds__seed_csv = """id
1
"""

empty_schema_with_version_yml = """
version: 2

"""

schema_sources5_yml = """
version: 2

sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            tests:
              - not_null:
                  severity: "{{ 'error' if target.name == 'prod' else 'warn' }}"
              - unique
          - name: first_name
          - name: last_name
          - name: email

seeds:
  - name: rad_customers
    description: "Raw customer data"
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: first_name
      - name: last_name
      - name: email


"""

my_macro2_sql = """
{% macro do_something(foo2, bar2) %}

    select
        'foo' as foo2,
        'var' as bar2

{% endmacro %}

"""

raw_customers_csv = """id,first_name,last_name,email
1,Michael,Perez,mperez0@chronoengine.com
2,Shawn,Mccoy,smccoy1@reddit.com
3,Kathleen,Payne,kpayne2@cargocollective.com
4,Jimmy,Cooper,jcooper3@cargocollective.com
5,Katherine,Rice,krice4@typepad.com
6,Sarah,Ryan,sryan5@gnu.org
7,Martin,Mcdonald,mmcdonald6@opera.com
8,Frank,Robinson,frobinson7@wunderground.com
9,Jennifer,Franklin,jfranklin8@mail.ru
10,Henry,Welch,hwelch9@list-manage.com
"""

model_three_disabled2_sql = """
- Disabled model
{{ config(materialized='table', enabled=False) }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

"""

schema_sources4_yml = """
version: 2

sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            tests:
              - not_null:
                  severity: "{{ 'error' if target.name == 'prod' else 'warn' }}"
              - unique
              - every_value_is_blue
          - name: first_name
          - name: last_name
          - name: email

seeds:
  - name: raw_customers
    description: "Raw customer data"
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: first_name
      - name: last_name
      - name: email


"""

env_var_schema_yml = """
version: 2

models:
    - name: model_one
      config:
        materialized: "{{ env_var('TEST_SCHEMA_VAR') }}"

"""

my_test_sql = """
select
   * from {{ ref('customers') }} where first_name = '{{ macro_something() }}'

"""

empty_schema_yml = """

"""

schema_models_c_yml = """
version: 2

sources:
  - name: seed_source
    description: "This is a source override"
    overrides: local_dep
    schema: "{{ var('schema_override', target.schema) }}"
    tables:
      - name: "seed"
        columns:
          - name: id
            tests:
              - unique
              - not_null

"""

env_var_sources_yml = """
version: 2
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    database: "{{ env_var('ENV_VAR_DATABASE') }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            tests:
              - not_null:
                  severity: "{{ env_var('ENV_VAR_SEVERITY') }}"
              - unique
          - name: first_name
          - name: last_name
          - name: email



"""

generic_test_edited_sql = """
{% test is_odd(model, column_name) %}

with validation as (

    select
        {{ column_name }} as odd_field2

    from {{ model }}

),

validation_errors as (

    select
        odd_field2

    from validation
    -- if this is true, then odd_field is actually even!
    where (odd_field2 % 2) = 0

)

select *
from validation_errors

{% endtest %}
"""

schema_sources1_yml = """
version: 2
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            tests:
              - not_null:
                  severity: "{{ 'error' if target.name == 'prod' else 'warn' }}"
              - unique
          - name: first_name
          - name: last_name
          - name: email



"""

schema_sources3_yml = """
version: 2

sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            tests:
              - not_null:
                  severity: "{{ 'error' if target.name == 'prod' else 'warn' }}"
              - unique
          - name: first_name
          - name: last_name
          - name: email

exposures:
  - name: proxy_for_dashboard
    description: "This is for the XXX dashboard"
    type: "dashboard"
    owner:
      name: "Dashboard Tester"
      email: "tester@dashboard.com"
    depends_on:
      - ref("model_one")
      - source("seed_sources", "raw_customers")


"""

my_analysis_sql = """
select * from customers

"""

schema_sources2_yml = """
version: 2

sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            tests:
              - not_null:
                  severity: "{{ 'error' if target.name == 'prod' else 'warn' }}"
              - unique
          - name: first_name
          - name: last_name
          - name: email

exposures:
  - name: proxy_for_dashboard
    description: "This is for the XXX dashboard"
    type: "dashboard"
    owner:
      name: "Dashboard Tester"
      email: "tester@dashboard.com"
    depends_on:
      - ref("model_one")
      - ref("raw_customers")
      - source("seed_sources", "raw_customers")


"""

model_color_sql = """
select 'blue' as fun

"""

my_metric_yml = """
version: 2
metrics:
  - name: new_customers
    label: New Customers
    model: customers
    description: "The number of paid customers who are using the product"
    calculation_method: count
    expression: user_id
    timestamp: signup_date
    time_grains: [day, week, month]
    dimensions:
      - plan
      - country
    filters:
      - field: is_paying
        value: True
        operator: '='
    +meta:
        is_okr: True
    tags:
      - okrs



"""

env_var_schema2_yml = """
version: 2

models:
    - name: model_one
      config:
        materialized: "{{ env_var('TEST_SCHEMA_VAR') }}"
      tests:
        - check_color:
            column_name: fun
            color: "env_var('ENV_VAR_COLOR')"


"""

gsm_override_sql = """
- custom macro
{% macro generate_schema_name(schema_name, node) %}

    {{ schema_name }}_{{ target.schema }}

{% endmacro %}

"""

model_four1_sql = """
select * from {{ ref('model_three') }}

"""

model_one_sql = """
select 1 as fun

"""

env_var_schema3_yml = """
version: 2

models:
    - name: model_one
      config:
        materialized: "{{ env_var('TEST_SCHEMA_VAR') }}"
      tests:
        - check_color:
            column_name: fun
            color: "env_var('ENV_VAR_COLOR')"

exposures:
  - name: proxy_for_dashboard
    description: "This is for the XXX dashboard"
    type: "dashboard"
    owner:
      name: "{{ env_var('ENV_VAR_OWNER') }}"
      email: "tester@dashboard.com"
    depends_on:
      - ref("model_color")
      - source("seed_sources", "raw_customers")

"""

env_var_metrics_yml = """
version: 2

metrics:

  - model: "ref('people')"
    name: number_of_people
    description: Total count of people
    label: "Number of people"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: '{{ env_var("ENV_VAR_METRICS") }}'

  - model: "ref('people')"
    name: collective_tenure
    description: Total number of years of team experience
    label: "Collective tenure"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: is
        value: 'true'

"""

customers_sql = """
with source as (

    select * from {{ source('seed_sources', 'raw_customers') }}

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name,
        email

    from source

)

select * from renamed

"""

model_four2_sql = """
select fun from {{ ref('model_one') }}

"""

env_var_model_sql = """
select '{{ env_var('ENV_VAR_TEST') }}' as vartest

"""

env_var_model_one_sql = """
select 'blue' as fun

"""

custom_schema_tests2_sql = """
{% test type_one(model) %}

    select * from (

        select * from {{ model }}
        union all
        select * from {{ ref('model_b') }}

    ) as Foo

{% endtest %}

{% test type_two(model) %}

    {{ config(severity = "ERROR") }}

    select * from {{ model }}

{% endtest %}

"""

metric_model_a_sql = """
{%
    set metric_list = [
        metric('number_of_people'),
        metric('collective_tenure')
    ]
%}

{% if not execute %}

    {% set metric_names = [] %}
    {% for m in metric_list %}
        {% do metric_names.append(m.metric_name) %}
    {% endfor %}

    -- this config does nothing, but it lets us check these values
    {{ config(metric_names = metric_names) }}

{% endif %}


select 1 as fun

"""

model_b_sql = """
select 1 as notfun

"""

customers2_md = """
{% docs customer_table %}

LOTS of customer data

{% enddocs %}

"""

custom_schema_tests1_sql = """
{% test type_one(model) %}

    select * from (

        select * from {{ model }}
        union all
        select * from {{ ref('model_b') }}

    ) as Foo

{% endtest %}

{% test type_two(model) %}

    {{ config(severity = "WARN") }}

    select * from {{ model }}

{% endtest %}

"""

people_metrics_yml = """
version: 2

metrics:

  - model: "ref('people')"
    name: number_of_people
    description: Total count of people
    label: "Number of people"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'testing'

  - model: "ref('people')"
    name: collective_tenure
    description: Total number of years of team experience
    label: "Collective tenure"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: is
        value: 'true'

"""

people_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at

"""

orders_sql = """
select 1 as id, 101 as user_id, 'pending' as status

"""

model_a_sql = """
select 1 as fun

"""

model_three_disabled_sql = """
{{ config(materialized='table', enabled=False) }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

"""

models_schema2b_yml = """
version: 2

models:
    - name: model_one
      description: "The first model"
    - name: model_three
      description: "The third model"
      columns:
        - name: id
          tests:
            - not_null

"""

env_var_macros_yml = """
version: 2
macros:
    - name: do_something
      description: "This is a test macro"
      meta:
          some_key: "{{ env_var('ENV_VAR_SOME_KEY') }}"


"""

models_schema4_yml = """
version: 2

models:
    - name: model_one
      description: "The first model"
    - name: model_three
      description: "The third model"
      config:
        enabled: false
      columns:
        - name: id
          tests:
            - unique

"""

model_two_sql = """
select 1 as notfun

"""

generic_test_schema_yml = """
version: 2

models:
  - name: orders
    description: "Some order data"
    columns:
      - name: id
        tests:
          - unique
          - is_odd

"""

customers1_md = """
{% docs customer_table %}

This table contains customer data

{% enddocs %}

"""

model_three_modified_sql = """
{{ config(materialized='table') }}

with source_data as (

    {#- This is model three #}

    select 1 as id
    union all
    select null as id

)

select *
from source_data

"""

macros_yml = """
version: 2
macros:
    - name: do_something
      description: "This is a test macro"

"""

test_color_sql = """
{% test check_color(model, column_name, color) %}

    select *
    from {{ model }}
    where {{ column_name }} = '{{ color }}'

{% endtest %}

"""

models_schema2_yml = """
version: 2

models:
    - name: model_one
      description: "The first model"
    - name: model_three
      description: "The third model"
      columns:
        - name: id
          tests:
            - unique

"""

gsm_override2_sql = """
- custom macro xxxx
{% macro generate_schema_name(schema_name, node) %}

    {{ schema_name }}_{{ target.schema }}

{% endmacro %}

"""

models_schema3_yml = """
version: 2

models:
    - name: model_one
      description: "The first model"
    - name: model_three
      description: "The third model"
      tests:
          - unique
macros:
    - name: do_something
      description: "This is a test macro"

"""

generic_test_sql = """
{% test is_odd(model, column_name) %}

with validation as (

    select
        {{ column_name }} as odd_field

    from {{ model }}

),

validation_errors as (

    select
        odd_field

    from validation
    -- if this is true, then odd_field is actually even!
    where (odd_field % 2) = 0

)

select *
from validation_errors

{% endtest %}
"""

env_var_model_test_yml = """
version: 2
models:
  - name: model_color
    columns:
      - name: fun
        tests:
          - unique:
              enabled: "{{ env_var('ENV_VAR_ENABLED', True) }}"

"""

model_three_sql = """
{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

"""

ref_override2_sql = """
- Macro to override ref xxxx
{% macro ref(modelname) %}
{% do return(builtins.ref(modelname)) %}
{% endmacro %}

"""

models_schema1_yml = """
version: 2

models:
    - name: model_one
      description: "The first model"

"""

macros_schema_yml = """

version: 2

models:
    - name: model_a
      tests:
        - type_one
        - type_two

"""

my_macro_sql = """
{% macro do_something(foo2, bar2) %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2

{% endmacro %}

"""

snapshot_sql = """
{% snapshot orders_snapshot %}

{{
    config(
      target_schema=schema,
      strategy='check',
      unique_key='id',
      check_cols=['status'],
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}

{% snapshot orders2_snapshot %}

{{
    config(
      target_schema=schema,
      strategy='check',
      unique_key='id',
      check_cols=['order_date'],
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}

"""

models_schema4b_yml = """
version: 2

models:
    - name: model_one
      description: "The first model"
    - name: model_three
      description: "The third model"
      config:
        enabled: true
      columns:
        - name: id
          tests:
            - unique

"""

test_macro_sql = """
{% macro macro_something() %}

    {% do return('macro_something') %}

{% endmacro %}

"""

people_metrics2_yml = """
version: 2

metrics:

  - model: "ref('people')"
    name: number_of_people
    description: Total count of people
    label: "Number of people"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'replaced'

  - model: "ref('people')"
    name: collective_tenure
    description: Total number of years of team experience
    label: "Collective tenure"
    calculation_method: sum
    expression: tenure
    timestamp: created_at
    time_grains: [day]
    filters:
      - field: loves_dbt
        operator: is
        value: 'true'

"""

generic_schema_yml = """
version: 2

models:
  - name: orders
    description: "Some order data"
    columns:
      - name: id
        tests:
          - unique

"""


groups_schema_yml_one_group = """
version: 2

groups:
  - name: test_group
    owner:
      name: test_group_owner

models:
  - name: orders
    description: "Some order data"
"""


groups_schema_yml_two_groups = """
version: 2

groups:
  - name: test_group
    owner:
      name: test_group_owner
  - name: test_group2
    owner:
      name: test_group_owner2

models:
  - name: orders
    description: "Some order data"
"""

groups_schema_yml_one_group_model_in_group2 = """
version: 2

groups:
  - name: test_group
    owner:
      name: test_group_owner

models:
  - name: orders
    description: "Some order data"
    config:
      group: test_group2
"""

groups_schema_yml_two_groups_edited = """
version: 2

groups:
  - name: test_group
    owner:
      name: test_group_owner
  - name: test_group2_edited
    owner:
      name: test_group_owner2

models:
  - name: orders
    description: "Some order data"
"""


snapshot2_sql = """
- add a comment
{% snapshot orders_snapshot %}

{{
    config(
      target_schema=schema,
      strategy='check',
      unique_key='id',
      check_cols=['status'],
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}

{% snapshot orders2_snapshot %}

{{
    config(
      target_schema=schema,
      strategy='check',
      unique_key='id',
      check_cols=['order_date'],
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}

"""

sources_tests2_sql = """

{% test every_value_is_blue(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} != 99

{% endtest %}


"""

people_metrics3_yml = """
version: 2

metrics:

  - model: "ref('people')"
    name: number_of_people
    description: Total count of people
    label: "Number of people"
    calculation_method: count
    expression: "*"
    timestamp: created_at
    time_grains: [day, week, month]
    dimensions:
      - favorite_color
      - loves_dbt
    meta:
        my_meta: 'replaced'

"""

ref_override_sql = """
- Macro to override ref
{% macro ref(modelname) %}
{% do return(builtins.ref(modelname)) %}
{% endmacro %}

"""

test_macro2_sql = """
{% macro macro_something() %}

    {% do return('some_name') %}

{% endmacro %}

"""

env_var_macro_sql = """
{% macro do_something(foo2, bar2) %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2

{% endmacro %}

"""

sources_tests1_sql = """

{% test every_value_is_blue(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} = 9999

{% endtest %}


"""
