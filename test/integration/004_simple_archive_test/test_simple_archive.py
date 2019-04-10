from test.integration.base import DBTIntegrationTest, use_profile
import dbt.exceptions


class TestSimpleArchive(DBTIntegrationTest):
    NUM_ARCHIVE_MODELS = 1

    @property
    def schema(self):
        return "simple_archive_004"

    @property
    def models(self):
        return "test/integration/004_simple_archive_test/models"

    def run_archive(self):
        return self.run_dbt(['archive'])

    @property
    def project_config(self):
        source_table = 'seed'

        if self.adapter_type == 'snowflake':
            source_table = source_table.upper()

        return {
            "data-paths": ['test/integration/004_simple_archive_test/data'],
            "archive": [
                {
                    "source_schema": self.unique_schema(),
                    "target_schema": self.unique_schema(),
                    "tables": [
                        {
                            "source_table": source_table,
                            "target_table": "archive_actual",
                            "updated_at": 'updated_at',
                            "unique_key": '''id || '-' || first_name'''
                        },
                    ],
                },
            ],
        }

    def dbt_run_seed_archive(self):
        self.run_sql_file('test/integration/004_simple_archive_test/seed.sql')

        results = self.run_archive()
        self.assertEqual(len(results),  self.NUM_ARCHIVE_MODELS)

    def assert_case_tables_equal(self, actual, expected):
        if self.adapter_type == 'snowflake':
            actual = actual.upper()
            expected = expected.upper()

        self.assertTablesEqual(actual, expected)

    def assert_expected(self):
        self.assert_case_tables_equal('archive_actual', 'archive_expected')

    @use_profile('postgres')
    def test__postgres__simple_archive(self):
        self.dbt_run_seed_archive()

        self.assert_expected()

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_postgres.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_archive()
        self.assertEqual(len(results),  self.NUM_ARCHIVE_MODELS)

        self.assert_expected()

    @use_profile('snowflake')
    def test__snowflake__simple_archive(self):
        self.dbt_run_seed_archive()

        self.assert_expected()

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_snowflake.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_archive()
        self.assertEqual(len(results),  self.NUM_ARCHIVE_MODELS)

        self.assert_expected()

    @use_profile('redshift')
    def test__redshift__simple_archive(self):
        self.dbt_run_seed_archive()

        self.assert_expected()

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_postgres.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_archive()
        self.assertEqual(len(results),  self.NUM_ARCHIVE_MODELS)

        self.assert_expected()

    @use_profile('presto')
    def test__presto__simple_archive_disabled(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  self.NUM_ARCHIVE_MODELS)
        # presto does not run archives
        results = self.run_dbt(["archive"], expect_pass=False)
        self.assertEqual(len(results),  self.NUM_ARCHIVE_MODELS)
        self.assertIn('not implemented for presto', results[0].error)


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

    def assert_expected(self):
        self.assertTablesEqual('archive_actual', 'archive_expected')

    @use_profile('bigquery')
    def test__bigquery__simple_archive(self):
        self.use_default_project()
        self.use_profile('bigquery')

        self.run_sql_file("test/integration/004_simple_archive_test/seed_bq.sql")

        self.run_dbt(["archive"])

        self.assert_expected()

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_bigquery.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update_bq.sql")

        self.run_dbt(["archive"])

        self.assert_expected()


    @use_profile('bigquery')
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

        expected_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='archive_expected'
        )
        archived_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='archive_actual'
        )

        self.assertTrue(len(expected_cols) > 0, "source table does not exist -- bad test")
        self.assertEqual(len(expected_cols), len(archived_cols), "actual and expected column lengths are different")

        for (expected_col, actual_col) in zip(expected_cols, archived_cols):
            expected_name, expected_type, _ = expected_col
            actual_name, actual_type, _ = actual_col
            self.assertTrue(expected_name is not None)
            self.assertTrue(expected_type is not None)

            self.assertEqual(expected_name, actual_name, "names are different")
            self.assertEqual(expected_type, actual_type, "data types are different")


class TestCrossDBArchive(DBTIntegrationTest):
    setup_alternate_db = True
    @property
    def schema(self):
        return "simple_archive_004"

    @property
    def models(self):
        return "test/integration/004_simple_archive_test/models"

    @property
    def archive_project_config(self):
        if self.adapter_type == 'snowflake':
            return {
                "source_table": 'SEED',
                "target_table": "archive_actual",
                "updated_at": 'updated_at',
                "unique_key": '''id || '-' || first_name'''
            }
        else:
            return {
                "source_table": 'seed',
                "target_table": "archive_actual",
                "updated_at": 'updated_at',
                "unique_key": "concat(cast(id as string) , '-', first_name)"
            }

    @property
    def project_config(self):
        return {
            "archive": [
                {
                    'target_database': self.alternative_database,
                    "source_schema": self.unique_schema(),
                    "target_schema": self.unique_schema(),
                    "tables": [self.archive_project_config]
                }
            ]
        }

    def run_archive(self):
        return self.run_dbt(['archive'])

    @use_profile('snowflake')
    def test__snowflake__cross_archive(self):
        self.run_sql_file("test/integration/004_simple_archive_test/seed.sql")

        results = self.run_archive()
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("ARCHIVE_EXPECTED", "ARCHIVE_ACTUAL", table_b_db=self.alternative_database)

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_snowflake.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_archive()
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("ARCHIVE_EXPECTED", "ARCHIVE_ACTUAL", table_b_db=self.alternative_database)

    @use_profile('bigquery')
    def test__bigquery__cross_archive(self):
        self.run_sql_file("test/integration/004_simple_archive_test/seed_bq.sql")

        self.run_archive()

        self.assertTablesEqual("archive_expected", "archive_actual", table_b_db=self.alternative_database)

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_bigquery.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update_bq.sql")

        self.run_archive()

        self.assertTablesEqual("archive_expected", "archive_actual", table_b_db=self.alternative_database)


class TestSimpleArchiveFiles(TestSimpleArchive):
    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/004_simple_archive_test/data'],
            "archive-paths": ['test/integration/004_simple_archive_test/test-archives-pg'],
        }

    @use_profile('postgres')
    def test__postgres_ref_archive(self):
        self.dbt_run_seed_archive()
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)


class TestSimpleArchiveFileSelects(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_archive_004"

    @property
    def models(self):
        return "test/integration/004_simple_archive_test/models"

    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/004_simple_archive_test/data'],
            "archive-paths": ['test/integration/004_simple_archive_test/test-archives-select',
                              'test/integration/004_simple_archive_test/test-archives-pg'],
        }

    @use_profile('postgres')
    def test__postgres__select_archives(self):
        self.run_sql_file('test/integration/004_simple_archive_test/seed.sql')

        results = self.run_dbt(['archive'])
        self.assertEqual(len(results),  4)
        self.assertTablesEqual('archive_castillo', 'archive_castillo_expected')
        self.assertTablesEqual('archive_alvarez', 'archive_alvarez_expected')
        self.assertTablesEqual('archive_kelly', 'archive_kelly_expected')
        self.assertTablesEqual('archive_actual', 'archive_expected')

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_postgres.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update.sql")

        results = self.run_dbt(['archive'])
        self.assertEqual(len(results),  4)
        self.assertTablesEqual('archive_castillo', 'archive_castillo_expected')
        self.assertTablesEqual('archive_alvarez', 'archive_alvarez_expected')
        self.assertTablesEqual('archive_kelly', 'archive_kelly_expected')
        self.assertTablesEqual('archive_actual', 'archive_expected')

    @use_profile('postgres')
    def test__postgres_exclude_archives(self):
        self.run_sql_file('test/integration/004_simple_archive_test/seed.sql')
        results = self.run_dbt(['archive', '--exclude', 'archive_castillo'])
        self.assertEqual(len(results),  3)
        self.assertTableDoesNotExist('archive_castillo')
        self.assertTablesEqual('archive_alvarez', 'archive_alvarez_expected')
        self.assertTablesEqual('archive_kelly', 'archive_kelly_expected')
        self.assertTablesEqual('archive_actual', 'archive_expected')

    @use_profile('postgres')
    def test__postgres_select_archives(self):
        self.run_sql_file('test/integration/004_simple_archive_test/seed.sql')
        results = self.run_dbt(['archive', '--models', 'archive_castillo'])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual('archive_castillo', 'archive_castillo_expected')
        self.assertTableDoesNotExist('archive_alvarez')
        self.assertTableDoesNotExist('archive_kelly')
        self.assertTableDoesNotExist('archive_actual')


class TestSimpleArchiveFilesBigquery(TestSimpleArchiveBigquery):
    @property
    def project_config(self):
        return {
            "archive-paths": ['test/integration/004_simple_archive_test/test-archives-bq'],
        }


class TestCrossDBArchiveFiles(TestCrossDBArchive):
    @property
    def project_config(self):
        if self.adapter_type == 'snowflake':
            paths = ['test/integration/004_simple_archive_test/test-archives-pg']
        else:
            paths = ['test/integration/004_simple_archive_test/test-archives-bq']
        return {
            'archive-paths': paths,
        }

    def run_archive(self):
        return self.run_dbt(['archive', '--vars', '{{"target_database": {}}}'.format(self.alternative_database)])


class TestBadArchive(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_archive_004"

    @property
    def models(self):
        return "test/integration/004_simple_archive_test/models"

    @property
    def project_config(self):
        return {
            "archive-paths": ['test/integration/004_simple_archive_test/test-archives-invalid'],
        }

    @use_profile('postgres')
    def test__postgres__invalid(self):
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(['compile'], expect_pass=False)

        self.assertIn('target_database', str(exc.exception))


class TestCheckCols(TestSimpleArchiveFiles):
    NUM_ARCHIVE_MODELS = 2
    def _assertTablesEqualSql(self, relation_a, relation_b, columns=None):
        # When building the equality tests, only test columns that don't start
        # with 'dbt_', because those are time-sensitive
        if columns is None:
            columns = [c for c in self.get_relation_columns(relation_a) if not c[0].lower().startswith('dbt_')]
        return super(TestCheckCols, self)._assertTablesEqualSql(
            relation_a,
            relation_b,
            columns=columns
        )

    def assert_expected(self):
        super(TestCheckCols, self).assert_expected()
        self.assert_case_tables_equal('archive_checkall', 'archive_expected')

    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/004_simple_archive_test/data'],
            "archive-paths": ['test/integration/004_simple_archive_test/test-check-col-archives'],
        }


class TestCheckColsBigquery(TestSimpleArchiveFilesBigquery):
    def _assertTablesEqualSql(self, relation_a, relation_b, columns=None):
        # When building the equality tests, only test columns that don't start
        # with 'dbt_', because those are time-sensitive
        if columns is None:
            columns = [c for c in self.get_relation_columns(relation_a) if not c[0].lower().startswith('dbt_')]
        return super(TestCheckColsBigquery, self)._assertTablesEqualSql(
            relation_a,
            relation_b,
            columns=columns
        )

    def assert_expected(self):
        super(TestCheckColsBigquery, self).assert_expected()
        self.assertTablesEqual('archive_checkall', 'archive_expected')

    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/004_simple_archive_test/data'],
            "archive-paths": ['test/integration/004_simple_archive_test/test-check-col-archives-bq'],
        }

    @use_profile('bigquery')
    def test__bigquery__archive_with_new_field(self):
        self.use_default_project()
        self.use_profile('bigquery')

        self.run_sql_file("test/integration/004_simple_archive_test/seed_bq.sql")

        self.run_dbt(["archive"])

        self.assertTablesEqual("archive_expected", "archive_actual")
        self.assertTablesEqual("archive_expected", "archive_checkall")

        self.run_sql_file("test/integration/004_simple_archive_test/invalidate_bigquery.sql")
        self.run_sql_file("test/integration/004_simple_archive_test/update_bq.sql")

        # This adds new fields to the source table, and updates the expected archive output accordingly
        self.run_sql_file("test/integration/004_simple_archive_test/add_column_to_source_bq.sql")

        # this should fail because `check="all"` will try to compare the nested field
        self.run_dbt(['archive'], expect_pass=False)

        self.run_dbt(["archive", '-m', 'archive_actual'])

        # A more thorough test would assert that archived == expected, but BigQuery does not support the
        # "EXCEPT DISTINCT" operator on nested fields! Instead, just check that schemas are congruent.

        expected_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='archive_expected'
        )
        archived_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='archive_actual'
        )

        self.assertTrue(len(expected_cols) > 0, "source table does not exist -- bad test")
        self.assertEqual(len(expected_cols), len(archived_cols), "actual and expected column lengths are different")

        for (expected_col, actual_col) in zip(expected_cols, archived_cols):
            expected_name, expected_type, _ = expected_col
            actual_name, actual_type, _ = actual_col
            self.assertTrue(expected_name is not None)
            self.assertTrue(expected_type is not None)

            self.assertEqual(expected_name, actual_name, "names are different")
            self.assertEqual(expected_type, actual_type, "data types are different")
