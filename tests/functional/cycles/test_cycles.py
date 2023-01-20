import pytest

from dbt.tests.util import run_dbt

model_a_sql = """
select * from {{ ref('model_b') }}
"""

model_b_sql = """
select * from {{ ref('model_a') }}
"""

complex_cycle__model_a_sql = """
select 1 as id
"""

complex_cycle__model_b_sql = """
select * from {{ ref('model_a') }}s
union all
select * from {{ ref('model_e') }}
"""

complex_cycle__model_c_sql = """
select * from {{ ref('model_b') }}
"""

complex_cycle__model_d_sql = """
select * from {{ ref('model_c') }}
"""

complex_cycle__model_e_sql = """
select * from {{ ref('model_e') }}
"""


class TestSimpleCycle:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_a_sql, "model_b.sql": model_b_sql}

    def test_simple_cycle(self, project):
        with pytest.raises(RuntimeError) as exc:
            run_dbt(["run"])
        expected_msg = "Found a cycle"
        assert expected_msg in str(exc.value)


class TestComplexCycle:
    @pytest.fixture(scope="class")
    def models(self):
        # The cycle in this graph looks like:
        #   A -> B -> C -> D
        #        ^         |
        #        |         |
        #        +--- E <--+
        return {
            "model_a.sql": complex_cycle__model_a_sql,
            "model_b.sql": complex_cycle__model_b_sql,
            "model_c.sql": complex_cycle__model_c_sql,
            "model_d.sql": complex_cycle__model_d_sql,
            "model_e.sql": complex_cycle__model_e_sql,
        }

    def test_complex_cycle(self, project):
        with pytest.raises(RuntimeError) as exc:
            run_dbt(["run"])
        expected_msg = "Found a cycle"
        assert expected_msg in str(exc.value)
