from test.integration.base import DBTIntegrationTest, use_profile
from pytest import mark


class TestAdapterDDL(DBTIntegrationTest):
    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_dbt(["seed"])

    @property
    def schema(self):
        return "adapter_ddl_063"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "seeds": {
                "quote_columns": False,
            },
        }

    # 63 characters is the character limit for a table name in a postgres database
    # (assuming compiled without changes from source)
    @use_profile("postgres")
    def test_postgres_name_longer_than_63_fails(self):
        self.run_dbt(
            [
                "run",
                "-m",
                "my_name_is_64_characters_abcdefghijklmnopqrstuvwxyz0123456789012",
            ],
            expect_pass=False,
        )

    @mark.skip(
        reason="Backup table generation currently adds 12 characters to the relation name, meaning the current name limit is 51."
    )
    @use_profile("postgres")
    def test_postgres_name_shorter_or_equal_to_63_passes(self):
        self.run_dbt(
            [
                "run",
                "-m",
                "my_name_is_52_characters_abcdefghijklmnopqrstuvwxyz0"
                "my_name_is_63_characters_abcdefghijklmnopqrstuvwxyz012345678901",
            ],
            expect_pass=True,
        )

    @use_profile("postgres")
    def test_postgres_long_name_passes_when_temp_tables_are_generated(self):
        self.run_dbt(
            [
                "run",
                "-m",
                "my_name_is_51_characters_incremental_abcdefghijklmn",
            ],
            expect_pass=True,
        )

        # Run again to trigger incremental materialization
        self.run_dbt(
            [
                "run",
                "-m",
                "my_name_is_51_characters_incremental_abcdefghijklmn",
            ],
            expect_pass=True,
        )
