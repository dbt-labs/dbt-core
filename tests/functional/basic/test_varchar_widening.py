import os

import pytest

from dbt.tests.util import check_relations_equal
from tests.functional.v2_parser_parity.v2_self_parser import run_dbt_for_mode

incremental_sql = """
{{
  config(
    materialized = "incremental"
  )
}}

select * from {{ this.schema }}.seed

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}
"""

materialized_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed
"""


@pytest.fixture(scope="class")
def models():
    return {"incremental.sql": incremental_sql, "materialized.sql": materialized_sql}


@pytest.mark.v2_parser_parity
def test_varchar_widening(project, parser_mode):
    path = os.path.join(project.test_data_dir, "varchar10_seed.sql")
    project.run_sql_file(path)

    results = run_dbt_for_mode(parser_mode, ["run"])
    assert len(results) == 2

    check_relations_equal(project.adapter, ["seed", "incremental"])
    check_relations_equal(project.adapter, ["seed", "materialized"])

    path = os.path.join(project.test_data_dir, "varchar300_seed.sql")
    project.run_sql_file(path)

    results = run_dbt_for_mode(parser_mode, ["run"])
    assert len(results) == 2

    check_relations_equal(project.adapter, ["seed", "incremental"])
    check_relations_equal(project.adapter, ["seed", "materialized"])
