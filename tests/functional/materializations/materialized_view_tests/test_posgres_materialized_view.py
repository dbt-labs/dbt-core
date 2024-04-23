import pytest
from dbt.tests.util import run_dbt

SEED = """
order_id,customer_id,total_amount,order_date
1,101,50.00,2024-04-01
2,102,75.00,2024-04-02
3,103,100.00,2024-04-03
4,101,30.00,2024-04-04
5,104,45.00,2024-04-05
""".strip()

ORDERS = """
-- models/orders.sql
{{
  config(
    materialized='materialized_view'
  )
}}

SELECT
    order_id,
    customer_id,
    total_amount,
    order_date
FROM
    {{ ref('source_orders') }}
"""

PRODUCT_SALES = """
{{
  config(
    materialized='materialized_view'
  )
}}

SELECT
    order_id,
    SUM(total_amount) AS total_sales_amount
FROM
    {{ ref('orders') }}
GROUP BY
    order_id
"""


class TestPostgresTestRefreshMaterializedView:
    """
    this test addresses a issue in postgres around materialized views,
    and renaming against a model who has dependent models that are also materialized views
    related pr: https://github.com/dbt-labs/dbt-core/pull/9959
    """

    @pytest.fixture(scope="class")
    def models(self):
        yield {"orders.sql": ORDERS, "product_sales.sql": PRODUCT_SALES}

    @pytest.fixture(scope="class")
    def seeds(self):
        yield {"source_orders.csv": SEED}

    def test_postgres_refresh_dependent_naterialized_views(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])
        run_dbt(["run", "--full-refresh"])
