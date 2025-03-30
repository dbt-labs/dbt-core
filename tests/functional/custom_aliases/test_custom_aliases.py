import pytest

from dbt.tests.util import run_dbt
from tests.functional.custom_aliases.fixtures import (
    macros_config_sql,
    macros_sql,
    model1_sql,
    model2_sql,
    schema_yml,
)


class TestAliases:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model1.sql": model1_sql, "model2.sql": model2_sql, "schema.yml": schema_yml}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
        }

    def test_customer_alias_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test"])
        assert len(results) == 2


class TestAliasesWithConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model1.sql": model1_sql, "model2.sql": model2_sql, "schema.yml": schema_yml}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_config_sql,
        }

    def test_customer_alias_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test"])
        assert len(results) == 2
