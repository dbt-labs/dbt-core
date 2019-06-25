from test.integration.base import DBTIntegrationTest, use_profile
import dbt.exceptions


class TestSimpleSnapshotFiles(DBTIntegrationTest):
    NUM_SNAPSHOT_MODELS = 1

    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    def run_snapshot(self):
        return self.run_dbt(['snapshot'])

    @property
    def project_config(self):
        return {
            "data-paths": ['data'],
            "snapshot-paths": ['test-snapshots-pg'],
        }

    @use_profile('postgres')
    def test__postgres_ref_snapshot(self):
        self.dbt_run_seed_snapshot()
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)

    def dbt_run_seed_snapshot(self):
        if self.adapter_type == 'postgres':
            self.run_sql_file('seed_pg.sql')
        else:
            self.run_sql_file('seed.sql')

        results = self.run_snapshot()
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)

    def assert_case_tables_equal(self, actual, expected):
        if self.adapter_type == 'snowflake':
            actual = actual.upper()
            expected = expected.upper()

        self.assertTablesEqual(actual, expected)

    def assert_expected(self):
        self.assert_case_tables_equal('snapshot_actual', 'snapshot_expected')

    @use_profile('postgres')
    def test__postgres__simple_snapshot(self):
        self.dbt_run_seed_snapshot()

        self.assert_expected()

        self.run_sql_file("invalidate_postgres.sql")
        self.run_sql_file("update.sql")

        results = self.run_snapshot()
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)

        self.assert_expected()

    @use_profile('snowflake')
    def test__snowflake__simple_snapshot(self):
        self.dbt_run_seed_snapshot()

        self.assert_expected()

        self.run_sql_file("invalidate_snowflake.sql")
        self.run_sql_file("update.sql")

        results = self.run_snapshot()
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)

        self.assert_expected()

    @use_profile('redshift')
    def test__redshift__simple_snapshot(self):
        self.dbt_run_seed_snapshot()

        self.assert_expected()

        self.run_sql_file("invalidate_postgres.sql")
        self.run_sql_file("update.sql")

        results = self.run_snapshot()
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)

        self.assert_expected()

    @use_profile('presto')
    def test__presto__simple_snapshot_disabled(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)
        # presto does not run snapshots
        results = self.run_dbt(["snapshot"], expect_pass=False)
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)
        self.assertIn('not implemented for presto', results[0].error)


class TestSimpleSnapshotFileSelects(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "data-paths": ['data'],
            "snapshot-paths": ['test-snapshots-select',
                              'test-snapshots-pg'],
        }

    @use_profile('postgres')
    def test__postgres__select_snapshots(self):
        self.run_sql_file('seed_pg.sql')

        results = self.run_dbt(['snapshot'])
        self.assertEqual(len(results),  4)
        self.assertTablesEqual('snapshot_castillo', 'snapshot_castillo_expected')
        self.assertTablesEqual('snapshot_alvarez', 'snapshot_alvarez_expected')
        self.assertTablesEqual('snapshot_kelly', 'snapshot_kelly_expected')
        self.assertTablesEqual('snapshot_actual', 'snapshot_expected')

        self.run_sql_file("invalidate_postgres.sql")
        self.run_sql_file("update.sql")

        results = self.run_dbt(['snapshot'])
        self.assertEqual(len(results),  4)
        self.assertTablesEqual('snapshot_castillo', 'snapshot_castillo_expected')
        self.assertTablesEqual('snapshot_alvarez', 'snapshot_alvarez_expected')
        self.assertTablesEqual('snapshot_kelly', 'snapshot_kelly_expected')
        self.assertTablesEqual('snapshot_actual', 'snapshot_expected')

    @use_profile('postgres')
    def test__postgres_exclude_snapshots(self):
        self.run_sql_file('seed_pg.sql')
        results = self.run_dbt(['snapshot', '--exclude', 'snapshot_castillo'])
        self.assertEqual(len(results),  3)
        self.assertTableDoesNotExist('snapshot_castillo')
        self.assertTablesEqual('snapshot_alvarez', 'snapshot_alvarez_expected')
        self.assertTablesEqual('snapshot_kelly', 'snapshot_kelly_expected')
        self.assertTablesEqual('snapshot_actual', 'snapshot_expected')

    @use_profile('postgres')
    def test__postgres_select_snapshots(self):
        self.run_sql_file('seed_pg.sql')
        results = self.run_dbt(['snapshot', '--select', 'snapshot_castillo'])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual('snapshot_castillo', 'snapshot_castillo_expected')
        self.assertTableDoesNotExist('snapshot_alvarez')
        self.assertTableDoesNotExist('snapshot_kelly')
        self.assertTableDoesNotExist('snapshot_actual')


class TestSimpleSnapshotFilesBigquery(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "snapshot-paths": ['test-snapshots-bq'],
        }

    def assert_expected(self):
        self.assertTablesEqual('snapshot_actual', 'snapshot_expected')

    @use_profile('bigquery')
    def test__bigquery__simple_snapshot(self):
        self.use_default_project()
        self.use_profile('bigquery')

        self.run_sql_file("seed_bq.sql")

        self.run_dbt(["snapshot"])

        self.assert_expected()

        self.run_sql_file("invalidate_bigquery.sql")
        self.run_sql_file("update_bq.sql")

        self.run_dbt(["snapshot"])

        self.assert_expected()


    @use_profile('bigquery')
    def test__bigquery__snapshot_with_new_field(self):
        self.use_default_project()
        self.use_profile('bigquery')

        self.run_sql_file("seed_bq.sql")

        self.run_dbt(["snapshot"])

        self.assertTablesEqual("snapshot_expected", "snapshot_actual")

        self.run_sql_file("invalidate_bigquery.sql")
        self.run_sql_file("update_bq.sql")

        # This adds new fields to the source table, and updates the expected snapshot output accordingly
        self.run_sql_file("add_column_to_source_bq.sql")

        self.run_dbt(["snapshot"])

        # A more thorough test would assert that snapshotted == expected, but BigQuery does not support the
        # "EXCEPT DISTINCT" operator on nested fields! Instead, just check that schemas are congruent.

        expected_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='snapshot_expected'
        )
        snapshotted_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='snapshot_actual'
        )

        self.assertTrue(len(expected_cols) > 0, "source table does not exist -- bad test")
        self.assertEqual(len(expected_cols), len(snapshotted_cols), "actual and expected column lengths are different")

        for (expected_col, actual_col) in zip(expected_cols, snapshotted_cols):
            expected_name, expected_type, _ = expected_col
            actual_name, actual_type, _ = actual_col
            self.assertTrue(expected_name is not None)
            self.assertTrue(expected_type is not None)

            self.assertEqual(expected_name, actual_name, "names are different")
            self.assertEqual(expected_type, actual_type, "data types are different")


class TestCrossDBSnapshotFiles(DBTIntegrationTest):
    setup_alternate_db = True
    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        if self.adapter_type == 'snowflake':
            paths = ['test-snapshots-pg']
        else:
            paths = ['test-snapshots-bq']
        return {
            'snapshot-paths': paths,
        }

    def run_snapshot(self):
        return self.run_dbt(['snapshot', '--vars', '{{"target_database": {}}}'.format(self.alternative_database)])

    @use_profile('snowflake')
    def test__snowflake__cross_snapshot(self):
        self.run_sql_file("seed.sql")

        results = self.run_snapshot()
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("SNAPSHOT_EXPECTED", "SNAPSHOT_ACTUAL", table_b_db=self.alternative_database)

        self.run_sql_file("invalidate_snowflake.sql")
        self.run_sql_file("update.sql")

        results = self.run_snapshot()
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("SNAPSHOT_EXPECTED", "SNAPSHOT_ACTUAL", table_b_db=self.alternative_database)

    @use_profile('bigquery')
    def test__bigquery__cross_snapshot(self):
        self.run_sql_file("seed_bq.sql")

        self.run_snapshot()

        self.assertTablesEqual("snapshot_expected", "snapshot_actual", table_b_db=self.alternative_database)

        self.run_sql_file("invalidate_bigquery.sql")
        self.run_sql_file("update_bq.sql")

        self.run_snapshot()

        self.assertTablesEqual("snapshot_expected", "snapshot_actual", table_b_db=self.alternative_database)


class TestCrossSchemaSnapshotFiles(DBTIntegrationTest):
    NUM_SNAPSHOT_MODELS = 1

    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        paths = ['test-snapshots-pg']
        return {
            'snapshot-paths': paths,
        }

    def target_schema(self):
        return "{}_snapshotted".format(self.unique_schema())

    def run_snapshot(self):
        return self.run_dbt(['snapshot', '--vars', '{{"target_schema": {}}}'.format(self.target_schema())])

    @use_profile('postgres')
    def test__postgres__cross_schema_snapshot(self):
        self.run_sql_file('seed_pg.sql')

        results = self.run_snapshot()
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)

        results = self.run_dbt(['run', '--vars', '{{"target_schema": {}}}'.format(self.target_schema())])
        self.assertEqual(len(results), 1)


class TestBadSnapshot(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "snapshot-paths": ['test-snapshots-invalid'],
        }

    @use_profile('postgres')
    def test__postgres__invalid(self):
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(['compile'], expect_pass=False)

        self.assertIn('target_database', str(exc.exception))


class TestCheckCols(TestSimpleSnapshotFiles):
    NUM_SNAPSHOT_MODELS = 2

    def _assertTablesEqualSql(self, relation_a, relation_b, columns=None):
        # When building the equality tests, only test columns that don't start
        # with 'dbt_', because those are time-sensitive
        if columns is None:
            columns = [c for c in self.get_relation_columns(relation_a) if not c[0].lower().startswith('dbt_')]
        return super()._assertTablesEqualSql(relation_a, relation_b, columns=columns)

    def assert_expected(self):
        super().assert_expected()
        self.assert_case_tables_equal('snapshot_checkall', 'snapshot_expected')

    @property
    def project_config(self):
        return {
            "data-paths": ['data'],
            "snapshot-paths": ['test-check-col-snapshots'],
        }


class TestCheckColsBigquery(TestSimpleSnapshotFilesBigquery):
    def _assertTablesEqualSql(self, relation_a, relation_b, columns=None):
        # When building the equality tests, only test columns that don't start
        # with 'dbt_', because those are time-sensitive
        if columns is None:
            columns = [c for c in self.get_relation_columns(relation_a) if not c[0].lower().startswith('dbt_')]
        return super()._assertTablesEqualSql(relation_a, relation_b, columns=columns)

    def assert_expected(self):
        super().assert_expected()
        self.assertTablesEqual('snapshot_checkall', 'snapshot_expected')

    @property
    def project_config(self):
        return {
            "data-paths": ['data'],
            "snapshot-paths": ['test-check-col-snapshots-bq'],
        }

    @use_profile('bigquery')
    def test__bigquery__snapshot_with_new_field(self):
        self.use_default_project()
        self.use_profile('bigquery')

        self.run_sql_file("seed_bq.sql")

        self.run_dbt(["snapshot"])

        self.assertTablesEqual("snapshot_expected", "snapshot_actual")
        self.assertTablesEqual("snapshot_expected", "snapshot_checkall")

        self.run_sql_file("invalidate_bigquery.sql")
        self.run_sql_file("update_bq.sql")

        # This adds new fields to the source table, and updates the expected snapshot output accordingly
        self.run_sql_file("add_column_to_source_bq.sql")

        # this should fail because `check="all"` will try to compare the nested field
        self.run_dbt(['snapshot'], expect_pass=False)

        self.run_dbt(["snapshot", '--select', 'snapshot_actual'])

        # A more thorough test would assert that snapshotted == expected, but BigQuery does not support the
        # "EXCEPT DISTINCT" operator on nested fields! Instead, just check that schemas are congruent.

        expected_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='snapshot_expected'
        )
        snapshotted_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='snapshot_actual'
        )

        self.assertTrue(len(expected_cols) > 0, "source table does not exist -- bad test")
        self.assertEqual(len(expected_cols), len(snapshotted_cols), "actual and expected column lengths are different")

        for (expected_col, actual_col) in zip(expected_cols, snapshotted_cols):
            expected_name, expected_type, _ = expected_col
            actual_name, actual_type, _ = actual_col
            self.assertTrue(expected_name is not None)
            self.assertTrue(expected_type is not None)

            self.assertEqual(expected_name, actual_name, "names are different")
            self.assertEqual(expected_type, actual_type, "data types are different")


class TestLongText(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    def run_snapshot(self):
        return self.run_dbt(['snapshot'])

    @property
    def project_config(self):
        return {
            "snapshot-paths": ['test-snapshots-longtext'],
        }

    @use_profile('postgres')
    def test__postgres__long_text(self):
        self.run_sql_file('seed_longtext.sql')
        results = self.run_dbt(['snapshot'])
        self.assertEqual(len(results), 1)

        with self.adapter.connection_named('test'):
            status, results = self.adapter.execute(
                'select * from {}.{}.snapshot_actual'.format(self.default_database, self.unique_schema()),
                fetch=True
            )
        self.assertEqual(len(results), 2)
        got_names = set(r.get('longstring') for r in results)
        self.assertEqual(got_names, {'a' * 500, 'short'})
