from tests.integration.base import DBTIntegrationTest, use_profile
import os
import re


class TestEphemeralMulti(DBTIntegrationTest):
    @property
    def schema(self):
        return "ephemeral"

    @property
    def models(self):
        return "models"

    @use_profile('snowflake')
    def test__snowflake(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.assertManyTablesEqual(
            ["SEED", "DEPENDENT", "DOUBLE_DEPENDENT", "SUPER_DEPENDENT"]
        )
