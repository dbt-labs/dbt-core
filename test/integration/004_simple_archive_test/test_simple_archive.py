from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestSimpleArchive(DBTIntegrationTest):

    def setUp(self):
        pass

    @property
    def schema(self):
        return "simple_archive_004"

    @property
    def models(self):
        return "test/integration/004_simple_archive_test/models"

    @property
    def project_config(self):
        return {
            "archive": [
                {
                    "source_schema": "simple_archive_004",
                    "target_schema": "simple_archive_004",
                    "tables": [
                        {
                            "source_table": "seed",
                            "target_table": "archive_actual",
                            "updated_at": '"updated_at"',
                            "unique_key": '''"id" || '-' || "first_name"'''
                        }
                    ]
                }
            ]
        }

    @attr(type='postgres')
    def test__postgres__simple_archive(self):
        self.use_default_project()
        self.use_profile('postgres')
        self.run_sql_file("test/integration/004_simple_archive_test/seed.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected","archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_postgres.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected","archive_actual")

    @attr(type='snowflake')
    def test__snowflake__simple_archive(self):
        self.use_default_project()
        self.use_profile('snowflake')
        self.run_sql_file("test/integration/004_simple_archive_test/seed.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected","archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_snowflake.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected","archive_actual")
