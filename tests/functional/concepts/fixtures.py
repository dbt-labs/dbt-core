"""Fixtures for concept functional tests."""

# Basic concept definition with joins
basic_concept_yml = """
version: 2

concepts:
  - name: orders
    description: "Orders concept with customer data"
    base_model: stg_orders
    primary_key: order_id
    columns:
      - name: order_id
        description: "Primary key for orders"
      - name: customer_id
        description: "Foreign key to customers"
      - name: order_date
        description: "Date when order was placed"
      - name: status
        description: "Order status"
    joins:
      - name: stg_customers
        base_key: customer_id
        foreign_key: id
        alias: customer
        columns:
          - name: customer_name
            description: "Customer name"
          - name: email
            description: "Customer email"
"""

# Base staging models
stg_orders_sql = """
select * from {{ ref('raw_orders') }}
"""

stg_customers_sql = """
select * from {{ ref('raw_customers') }}
"""

# Model using cref
orders_report_sql = """
select
    order_id,
    order_date,
    customer_name
from {{ cref('orders', ['order_id', 'order_date', 'customer_name']) }}
where order_date >= '2023-01-01'
"""

# Seed data
raw_orders_csv = """order_id,customer_id,order_date,status
1,1,2023-01-01,completed
2,2,2023-01-02,pending
3,1,2023-01-03,completed
4,3,2023-01-04,cancelled
"""

raw_customers_csv = """id,customer_name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Charlie,charlie@example.com
"""

# Concept with only base columns (no joins)
simple_concept_yml = """
version: 2

concepts:
  - name: simple_orders
    description: "Simple orders concept with only base columns"
    base_model: stg_orders
    primary_key: order_id
    columns:
      - name: order_id
      - name: customer_id
      - name: order_date
      - name: status
"""

# Invalid concept with missing base_model
invalid_concept_yml = """
version: 2

concepts:
  - name: invalid_orders
    description: "Invalid concept"
    columns:
      - name: order_id
"""

# Concept with multiple joins
multi_join_concept_yml = """
version: 2

concepts:
  - name: enriched_orders
    description: "Orders with customer and product data"
    base_model: stg_orders
    primary_key: order_id
    columns:
      - name: order_id
      - name: customer_id
      - name: order_date
      - name: status
    joins:
      - name: stg_customers
        base_key: customer_id
        foreign_key: id
        alias: customer
        columns:
          - name: customer_name
          - name: email
      - name: stg_products
        base_key: product_id
        foreign_key: id
        alias: product
        columns:
          - name: product_name
          - name: price
"""

# Additional staging model for multi-join test
stg_products_sql = """
select * from {{ ref('raw_products') }}
"""

# Additional seed for multi-join test
raw_products_csv = """id,product_name,price
1,Widget,10.00
2,Gadget,20.00
3,Doohickey,15.00
"""

# Model using multi-join concept with partial columns
partial_join_model_sql = """
select
    order_id,
    customer_name,
    product_name
from {{ cref('enriched_orders', ['order_id', 'customer_name', 'product_name']) }}
"""

# Model using only base columns (should generate no joins)
base_only_model_sql = """
select
    order_id,
    order_date
from {{ cref('orders', ['order_id', 'order_date']) }}
"""
