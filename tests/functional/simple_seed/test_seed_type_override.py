import pytest

from dbt.tests.util import run_dbt

from tests.functional.simple_seed.fixtures import (
    macros__schema_test,
    properties__schema_yml,
    seeds__disabled_in_config,
    seeds__enabled_in_config,
    seeds__tricky,
)


class SimpleSeedColumnOverride(object):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": properties__schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_enabled.csv": seeds__enabled_in_config,
            "seed_disabled.csv": seeds__disabled_in_config,
            "seed_tricky.csv": seeds__tricky,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"schema_test.sql": macros__schema_test}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "enabled": False,
                    "quote_columns": True,
                    "seed_enabled": {"enabled": True, "+column_types": self.seed_enabled_types()},
                    "seed_tricky": {
                        "enabled": True,
                        "+column_types": self.seed_tricky_types(),
                    },
                },
            },
        }

    def seed_enabled_types(self):
        return {
            "seed_id": "text",
            "birthday": "date",
        }

    def seed_tricky_types(self):
        return {
            "seed_id_str": "text",
            "looks_like_a_bool": "text",
            "looks_like_a_date": "text",
        }

    def test_simple_seed_with_column_override(self, project):
        results = run_dbt(["seed", "--show"])
        len(results) == 2
        results = run_dbt(["test"])
        len(results) == 10


class TestSimpleSeedColumnOverride(SimpleSeedColumnOverride):
    pass
