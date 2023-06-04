import pytest
from dbt.tests.adapter.utils.base_utils import macros__equals_sql
from dbt.tests.adapter.utils.fixture_equals import (
    SEEDS__DATA_EQUALS_CSV,
    MODELS__EQUAL_VALUES_SQL,
    MODELS__NOT_EQUAL_VALUES_SQL,
)
from dbt.tests.util import run_dbt, relation_from_name


class BaseEquals:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "equals.sql": macros__equals_sql,
        }

    # make it possible to dynamically update the macro call with a namespace
    # (e.g.) 'dateadd', 'dbt.dateadd', 'dbt_utils.dateadd'
    def macro_namespace(self):
        return ""

    def interpolate_macro_namespace(self, model_sql, macro_name):
        macro_namespace = self.macro_namespace()
        return (
            model_sql.replace(f"{macro_name}(", f"{macro_namespace}.{macro_name}(")
            if macro_namespace
            else model_sql
        )

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_equals.csv": SEEDS__DATA_EQUALS_CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "equal_values.sql": self.interpolate_macro_namespace(
                MODELS__EQUAL_VALUES_SQL, "equals"
            ),
            "not_equal_values.sql": self.interpolate_macro_namespace(
                MODELS__NOT_EQUAL_VALUES_SQL, "equals"
            ),
        }

    def test_equal_values(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # There are 9 cases total; 3 are equal and 6 are not equal

        # 3 are equal
        relation = relation_from_name(project.adapter, "equal_values")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where expected = 'same'", fetch="one"
        )
        assert result[0] == 3

        # 6 are not equal
        relation = relation_from_name(project.adapter, "not_equal_values")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where expected = 'different'",
            fetch="one",
        )
        assert result[0] == 6


class TestEquals(BaseEquals):
    pass
