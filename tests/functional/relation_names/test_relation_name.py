import pytest

from dbt.contracts.results import RunStatus
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
        self.incremental_filename = "my_name_is_51_characters_incremental_abcdefghijklmn"
        # length is 63
        self.max_length_filename = "my_name_is_max_length_chars_abcdefghijklmnopqrstuvwxyz123456789"
        # length is 64
        self.over_max_length_filename = "my_name_is_one_over_max_length_chats_abcdefghijklmnopqrstuvwxyz1"

        self.filename_for_backup_file = "my_name_is_52_characters_abcdefghijklmnopqrstuvwxyz0"

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{self.incremental_filename}.sql":
                models__basic_incremental,
            f"{self.filename_for_backup_file}.sql":
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

    # Backup table name generation:
    #   1. for len(relation name) <= 51, backfills
    #   2. for len(relation name) > 51 characters, overwrites
    #  the last 12 characters with __dbt_backup
    def test_name_shorter_or_equal_to_63_passes(self, project):
        run_dbt(
            [
                "run",
                "-s",
                f"{self.max_length_filename}",
                f"{self.filename_for_backup_file}",
            ],
        )

    def test_long_name_passes_when_temp_tables_are_generated(self):
        run_dbt(
            [
                "run",
                "-s",
                f"{self.incremental_filename}",
            ],
        )

        # Run again to trigger incremental materialization
        run_dbt(
            [
                "run",
                "-s",
                f"{self.incremental_filename}",
            ],
        )

    # 63 characters is the character limit for a table name in a postgres database
    # (assuming compiled without changes from source)
    def test_name_longer_than_63_does_not_build(self):
        err_msg = "Relation name 'my_name_is_one_over_max"\
            "_length_chats_abcdefghijklmnopqrstuvwxyz1' is longer than 63 characters"
        res = run_dbt(
            [
                "run",
                "-s",
                self.over_max_length_filename,
            ],
            expect_pass=False
        )
        assert res[0].status == RunStatus.Error
        assert err_msg in res[0].message
