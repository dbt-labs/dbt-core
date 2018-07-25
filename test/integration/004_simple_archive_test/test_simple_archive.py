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

        results = self.run_dbt(["archive"])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("archive_expected","archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_postgres.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_dbt(["archive"])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("archive_expected","archive_actual")

    @attr(type='snowflake')
    def test__snowflake__simple_archive(self):
        self.use_profile('snowflake')
        self.use_default_project()
        self.run_sql_file("test/integration/004_simple_archive_test/seed.sql")

        results = self.run_dbt(["archive"])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("ARCHIVE_EXPECTED", "archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_snowflake.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_dbt(["archive"])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("ARCHIVE_EXPECTED", "archive_actual")

    @attr(type='redshift')
    def test__redshift__simple_archive(self):
        self.use_profile('redshift')
        self.use_default_project()
        self.run_sql_file("test/integration/004_simple_archive_test/seed.sql")

        results = self.run_dbt(["archive"])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("archive_expected","archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_postgres.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_dbt(["archive"])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("archive_expected","archive_actual")

class TestSimpleArchiveBigquery(DBTIntegrationTest):

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


    @attr(type='bigquery')
    def test__bigquery__archive_with_new_field(self):
        self.use_default_project()
        self.use_profile('bigquery')

        self.run_sql_file("test/integration/004_simple_archive_test/seed_bq.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected", "archive_actual")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_bigquery.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update_bq.sql")

        # This adds new fields to the source table, and updates the expected archive output accordingly
        self.run_sql_file("test/integration/004_simple_archive_test/add_column_to_source_bq.sql")

        self.run_dbt(["archive"])

        # A more thorough test would assert that archived == expected, but BigQuery does not support the
        # "EXCEPT DISTINCT" operator on nested fields! Instead, just check that schemas are congruent.

        expected_cols = self.adapter.get_columns_in_table(self._profile, self.project_config, self.unique_schema(), 'archive_expected')
        archived_cols = self.adapter.get_columns_in_table(self._profile, self.project_config, self.unique_schema(), 'archive_actual')

        self.assertTrue(len(expected_cols) > 0, "source table does not exist -- bad test")
        self.assertEqual(len(expected_cols), len(archived_cols), "actual and expected column lengths are different")

        expected_cols = sorted(expected_cols, key=lambda x: x.name)
        archived_cols = sorted(archived_cols, key=lambda x: x.name)

        for (expected_col, actual_col) in zip(expected_cols, archived_cols):
            self.assertTrue(expected_col.name is not None)
            self.assertTrue(expected_col.data_type is not None)

            self.assertEqual(expected_col.name, actual_col.name, "names are different")
            self.assertEqual(expected_col.data_type, actual_col.data_type, "data types are different")
