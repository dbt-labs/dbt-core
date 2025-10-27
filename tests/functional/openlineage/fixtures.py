import os

import pytest

from dbt.tests.util import run_dbt
from tests.fixtures.jaffle_shop import (
    JaffleShopProject,
    customers_sql,
    docs_md,
    get_jaffle_shop_seeds,
    orders_sql,
    overview_md,
    staging_stg_customers_sql,
    staging_stg_orders_sql,
    staging_stg_payments_sql,
)

# snapshots/orders_snapshot.sql
snapshot_orders_snapshot = """
{% snapshot orders_snapshot %}

        {{
            config(
              target_schema='snapshots',
              strategy='timestamp',
              unique_key='id',
              updated_at='order_date',
            )
        }}

         select * from {{ ref('raw_orders') }}

{% endsnapshot %}
"""

# snapshots/schema.yml
snapshot_yml = """
version: 2
snapshots:
  - name: orders_snapshot
    description: snapshot view of orders table
    columns:
      - name: order_id
        description: This is a unique identifier for a customer
        data_type: integer

  - name: orders_snapshot_with_syntax_error
    description: dummy snapshot view of orders table that has a syntax error
    columns:
      - name: order_id
        description: This is a unique identifier for a customer
        data_type: integer

"""

# models/staging/schema.yml
staging_schema_yml = """
version: 2

models:
  - name: stg_customers
    columns:
      - name: customer_id
        data_type: integer
        data_tests:
          - unique
          - not_null

  - name: stg_orders
    columns:
      - name: order_id
        data_type: integer
        type: integer
        dataType: integer
        data_tests:
          - unique
          - not_null
      - name: status
        data_type: string
        data_tests:
          - accepted_values:
              values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']

  - name: stg_payments
    columns:
      - name: payment_id
        data_type: integer
        data_tests:
          - unique
          - not_null
      - name: payment_method
        data_type: string
        data_tests:
          - accepted_values:
              values: ['credit_card', 'coupon', 'bank_transfer', 'gift_card']
"""

# schema.yml
schema_yml = """
version: 2

models:
  - name: customers
    description: This table has basic information about a customer, as well as some derived facts based on a customer's orders

    columns:
      - name: customer_id
        data_type: integer
        description: This is a unique identifier for a customer
        data_tests:
          - unique
          - not_null

      - name: first_name
        data_type: string
        description: Customer's first name. PII.

      - name: last_name
        data_type: string
        description: Customer's last name. PII.

      - name: first_order
        data_type: date
        description: Date (UTC) of a customer's first order

      - name: most_recent_order
        data_type: date
        description: Date (UTC) of a customer's most recent order

      - name: number_of_orders
        data_type: integer
        description: Count of the number of orders a customer has placed

      - name: total_order_amount
        data_type: float
        description: Total value (AUD) of a customer's orders

  - name: orders
    description: This table has basic information about orders, as well as some derived facts based on payments

    columns:
      - name: order_id
        data_type: integer
        data_tests:
          - unique
          - not_null
        description: This is a unique identifier for an order

      - name: customer_id
        data_type: integer
        description: Foreign key to the customers table
        data_tests:
          - not_null
          - relationships:
              to: ref('customers')
              field: customer_id

      - name: order_date
        data_type: date
        description: Date (UTC) that the order was placed

      - name: status
        data_type: string
        description: '{{ doc("orders_status") }}'
        data_tests:
          - accepted_values:
              values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']

      - name: amount
        data_type: float
        description: Total amount (AUD) of the order
        data_tests:
          - not_null

      - name: credit_card_amount
        data_type: float
        description: Amount of the order (AUD) paid for by credit card
        data_tests:
          - not_null

      - name: coupon_amount
        data_type: float
        description: Amount of the order (AUD) paid for by coupon
        data_tests:
          - not_null

      - name: bank_transfer_amount
        data_type: float
        description: Amount of the order (AUD) paid for by bank transfer
        data_tests:
          - not_null

      - name: gift_card_amount
        data_type: float
        description: Amount of the order (AUD) paid for by gift card
        data_tests:
          - not_null

  - name: orders_with_syntax_error
    description: dummy model that has SQL syntax error
    columns:
      - name: order_id
        data_type: integer
        data_tests:
          - unique
          - not_null
        description: This is a unique identifier for an order

  - name: orders_with_failed_test
    description: dummy model that has failed test
    columns:
      - name: order_id
        data_type: integer
        data_tests:
          - accepted_values:
              values: [99]
"""

# models/orders_with_syntax_error.sql
orders_with_syntax_error_sql = """
{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

        select * from {{ ref('stg_orders') }}

),

SELECT order_id,
FROM orders

SQL Syntax Error here
"""

# snapshots/orders_snapshot_with_syntax_error.sql
snapshot_orders_with_syntax_error_sql = """
{% snapshot orders_snapshot_with_syntax_error_sql %}

{{
    config(
      target_schema='snapshots',
      strategy='timestamp',
      unique_key='id',
      updated_at='order_date',
    )
}}

SELECT * FROM {{ ref('raw_orders') }}

SQL Syntax Error here

{% endsnapshot %}
"""

# selectors.yml
selectors_yml = """

selectors:
  - name: jaffle_shop_models
    description: "selects the successful models of jaffle shop"
    definition:
      union:
        - +customers
        - +orders
"""

# seeds/raw_countries.csv
raw_countries_csv = """
country_name,country_code
Algeria,DZ
China, CN
France,FR
Mexico,MX
Poland,PL
United States,US
""".strip()

# seeds/seeds.yml
seeds_yml = """
version: 2

seeds:
  - name: raw_countries
    config:
      column_types:
        country_code: text
        country_name: text
"""


class OpenLineageJaffleShopProject(JaffleShopProject):

    @pytest.fixture(scope="class")
    def build_jaffle_shop_project(self, project):
        """
        Build the jaffle shop project
        """
        run_dbt(["build", "--selector", "jaffle_shop_models"])

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "customers.sql": customers_sql,
            "docs.md": docs_md,
            "orders.sql": orders_sql,
            "overview.md": overview_md,
            "schema.yml": schema_yml,
            "staging": {
                "schema.yml": staging_schema_yml,
                "stg_customers.sql": staging_stg_customers_sql,
                "stg_orders.sql": staging_stg_orders_sql,
                "stg_payments.sql": staging_stg_payments_sql,
            },
            # resources expected to fail
            "orders_with_syntax_error.sql": orders_with_syntax_error_sql,
            "orders_with_failed_test.sql": orders_sql,
        }

    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "postgres",
            "threads": 1,
            "host": "localhost",
            "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
            "user": os.getenv("POSTGRES_TEST_USER", "root"),
            "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
            "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "orders_snapshot.sql": snapshot_orders_snapshot,
            "snapshots.yml": snapshot_yml,
            # resources expected to fail
            "orders_snapshot_with_syntax_error.sql": snapshot_orders_with_syntax_error_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        jaffle_shop_seeds = get_jaffle_shop_seeds()
        jaffle_shop_seeds["raw_countries.csv"] = raw_countries_csv
        jaffle_shop_seeds["seeds.yml"] = seeds_yml
        return jaffle_shop_seeds
