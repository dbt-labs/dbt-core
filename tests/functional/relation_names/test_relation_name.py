import pytest

from dbt.tests.util import run_dbt

# Test coverage: A relation is a name for a database entity, i.e. a table or view. Every relation has
# a name. These tests verify the default Postgres rules for relation names are followed. Adapters
# may override connection rules and thus may have their own tests.

seeds__seed = """col_A,col_B
1,2
3,4
5,6
"""

models__basic_incremental = """
select * from {{ this.schema }}.seed

{{
  config({
    "unique_key": "col_A",
    "materialized": "incremental"
    })
}}
"""

models__basic_table = """
select * from {{ this.schema }}.seed

{{
  config({
    "materialized": "table"
    })
}}
"""


class TestGeneratedDDLNameRules:
    @classmethod
    def setup_class(self):
        # length is 63
        self.max_length_filename = "my_name_is_max_length_chars_abcdefghijklmnopqrstuvwxyz123456789"
        self.over_max_length_filename = self.max_length_filename + '0'

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_name_is_51_characters_incremental_abcdefghijklmn.sql":
                models__basic_incremental,
            "my_name_is_52_characters_abcdefghijklmnopqrstuvwxyz0.sql":
                models__basic_table,
            f"{self.max_length_filename}.sql":
                models__basic_table,
            f"{self.over_max_length_filename}.sql":
                models__basic_table,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    # 63 characters is the character limit for a table name in a postgres database
    # (assuming compiled without changes from source)
    def test_name_longer_than_63_does_not_build(self):
        run_dbt(
            [
                "run",
                "-m",
                self.over_max_length_filename,
            ],
            expect_pass=False,
        )

    # Backup table name generation:
    #   1. for a relation whose name is smaller than 51 characters, backfills
    #   2. for a relation whose name is larger than 51 characters, overwrites
    #  the last 12 characters with __dbt_backup
    def test_name_shorter_or_equal_to_63_passes(self, project):
        run_dbt(
            [
                "run",
                "-m",
                "my_name_is_63_characters_abcdefghijklmnopqrstuvwxyz012345678901",
                "my_name_is_52_characters_abcdefghijklmnopqrstuvwxyz0",
            ],
        )

    def test_long_name_passes_when_temp_tables_are_generated(self):
        run_dbt(
            [
                "run",
                "-m",
                "my_name_is_51_characters_incremental_abcdefghijklmn",
            ],
        )

        # Run again to trigger incremental materialization
        run_dbt(
            [
                "run",
                "-m",
                "my_name_is_51_characters_incremental_abcdefghijklmn",
            ],
            expect_pass=True,
        )
