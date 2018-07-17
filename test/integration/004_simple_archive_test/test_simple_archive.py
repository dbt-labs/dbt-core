from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestSimpleArchive(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_archive_004"

    @property
    def models(self):
        return "test/integration/004_simple_archive_test/models"

    @property
    def project_config(self):
        source_table = 'seed'

        if self.adapter_type == 'snowflake':
            source_table = source_table.upper()

        return {
            "archive": [
                {
                    "source_schema": self.unique_schema(),
                    "target_schema": self.unique_schema(),
                    "tables": [
                        {
                            "source_table": source_table,
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
        self.use_profile('postgres')
        self.use_default_project()
        self.run_sql_file("test/integration/004_simple_archive_test/seed.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected","archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_postgres.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected","archive_actual")

    @attr(type='snowflake')
    def test__snowflake__simple_archive(self):
        self.use_profile('snowflake')
        self.use_default_project()
        self.run_sql_file("test/integration/004_simple_archive_test/seed.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("ARCHIVE_EXPECTED", "archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_snowflake.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("ARCHIVE_EXPECTED", "archive_actual")


class TestSimpleArchive(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_archive_004"

    @property
    def models(self):
        return "test/integration/004_simple_archive_test/models"

    @property
    def project_config(self):
        source_table = 'seed'

        return {
            "archive": [
                {
                    "source_schema": self.unique_schema(),
                    "target_schema": self.unique_schema(),
                    "tables": [
                        {
                            "source_table": 'seed',
                            "target_table": "archive_actual",
                            "updated_at": 'updated_at',
                            "unique_key": "concat(cast(id as string) , '-', first_name)"
                        }
                    ]
                }
            ]
        }

    @attr(type='bigquery')
    def test__bigquery__simple_archive(self):
        self.use_default_project()
        self.use_profile('bigquery')

        self.run_sql_file("test/integration/004_simple_archive_test/seed_bq.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected", "archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_bigquery.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update_bq.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected", "archive_actual")

