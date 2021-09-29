from tests.integration.base import DBTIntegrationTest, use_profile
from datetime import datetime
import pytz
import dbt.exceptions


class BaseSimpleSnapshotTest(DBTIntegrationTest):
    NUM_SNAPSHOT_MODELS = 1

    @property
    def schema(self):
        return "simple_snapshot"

    @property
    def models(self):
        return "models"

    def run_snapshot(self):
        return self.run_dbt(['snapshot'])

    def dbt_run_seed_snapshot(self):
        self.run_sql_file('seed.sql')

        results = self.run_snapshot()
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)

    def assert_case_tables_equal(self, actual, expected):
        # this does something different on snowflake, but here it's just assertTablesEqual
        self.assertTablesEqual(actual, expected)

    def assert_expected(self):
        self.run_dbt(['test'])
        self.assert_case_tables_equal('snapshot_actual', 'snapshot_expected')


class TestSimpleSnapshotFiles(BaseSimpleSnapshotTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data'],
            "snapshot-paths": ['test-snapshots-pg'],
            'macro-paths': ['macros'],
        }


class TestSimpleColumnSnapshotFiles(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_snapshot"

    @property
    def models(self):
        return "models-checkall"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'macro-paths': ['custom-snapshot-macros', 'macros'],
            'snapshot-paths': ['test-snapshots-checkall'],
            'seeds': {
                'quote_columns': False,
            }
        }

    def _run_snapshot_test(self):
        self.run_dbt(['seed'])
        self.run_dbt(['snapshot'])
        database = self.default_database
        database = self.adapter.quote(database)
        results = self.run_sql(
            'select * from {}.{}.my_snapshot'.format(database, self.unique_schema()),
            fetch='all'
        )
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertEqual(len(result), 6)

        self.run_dbt(['snapshot', '--vars', '{seed_name: seed_newcol}'])
        results = self.run_sql(
            'select * from {}.{}.my_snapshot where last_name is not NULL'.format(database, self.unique_schema()),
            fetch='all'
        )
        self.assertEqual(len(results), 3)

        for result in results:
            # new column
            self.assertEqual(len(result), 7)
            self.assertIsNotNone(result[-1])

        results = self.run_sql(
            'select * from {}.{}.my_snapshot where last_name is NULL'.format(database, self.unique_schema()),
            fetch='all'
        )
        self.assertEqual(len(results), 3)
        for result in results:
            # new column
            self.assertEqual(len(result), 7)

    @use_profile('bigquery')
    def test_bigquery_renamed_source(self):
        self._run_snapshot_test()


class TestSimpleSnapshotFileSelects(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_snapshot"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data'],
            "snapshot-paths": ['test-snapshots-select',
                               'test-snapshots-pg'],
            'macro-paths': ['macros'],
        }


class TestConfiguredSnapshotFileSelects(TestSimpleSnapshotFileSelects):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data'],
            "snapshot-paths": ['test-snapshots-select-noconfig'],
            "snapshots": {
                "test": {
                    "target_schema": self.unique_schema(),
                    "unique_key": "id || '-' || first_name",
                    'strategy': 'timestamp',
                    'updated_at': 'updated_at',
                },
            },
            'macro-paths': ['macros'],
        }


class TestSimpleSnapshotFilesBigquery(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_snapshot"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "snapshot-paths": ['test-snapshots-bq'],
            'macro-paths': ['macros'],
        }

    def assert_expected(self):
        self.run_dbt(['test'])
        self.assertTablesEqual('snapshot_actual', 'snapshot_expected')

    @use_profile('bigquery')
    def test__bigquery__simple_snapshot(self):
        self.run_sql_file("seed_bq.sql")

        self.run_dbt(["snapshot"])

        self.assert_expected()

        self.run_sql_file("invalidate_bigquery.sql")
        self.run_sql_file("update_bq.sql")

        self.run_dbt(["snapshot"])

        self.assert_expected()

    @use_profile('bigquery')
    def test__bigquery__snapshot_with_new_field(self):

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
        return "simple_snapshot"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        paths = ['test-snapshots-bq']
        return {
            'config-version': 2,
            'snapshot-paths': paths,
            'macro-paths': ['macros'],
        }

    def run_snapshot(self):
        return self.run_dbt(['snapshot', '--vars', '{{"target_database": {}}}'.format(self.alternative_database)])

    @use_profile('bigquery')
    def test__bigquery__cross_snapshot(self):
        self.run_sql_file("seed_bq.sql")

        self.run_snapshot()

        self.assertTablesEqual("snapshot_expected", "snapshot_actual", table_b_db=self.alternative_database)

        self.run_sql_file("invalidate_bigquery.sql")
        self.run_sql_file("update_bq.sql")

        self.run_snapshot()

        self.assertTablesEqual("snapshot_expected", "snapshot_actual", table_b_db=self.alternative_database)


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
            'config-version': 2,
            "data-paths": ['data'],
            "snapshot-paths": ['test-check-col-snapshots'],
            'macro-paths': ['macros'],
        }


class TestConfiguredCheckCols(TestCheckCols):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data'],
            "snapshot-paths": ['test-check-col-snapshots-noconfig'],
            "snapshots": {
                "test": {
                    "target_schema": self.unique_schema(),
                    "unique_key": "id || '-' || first_name",
                    "strategy": "check",
                    "check_cols": ["email"],
                },
            },
            'macro-paths': ['macros'],
        }


class TestUpdatedAtCheckCols(TestCheckCols):
     def _assertTablesEqualSql(self, relation_a, relation_b, columns=None):
         revived_records = self.run_sql(
             '''
             select
                 id,
                 updated_at,
                 dbt_valid_from
             from {}
             '''.format(relation_b),
             fetch='all'
         )

         for result in revived_records:
             # result is a tuple, the updated_at is second and dbt_valid_from is latest
             self.assertIsInstance(result[1], datetime)
             self.assertIsInstance(result[2], datetime)
             self.assertEqual(result[1].replace(tzinfo=pytz.UTC), result[2].replace(tzinfo=pytz.UTC))

         if columns is None:
             columns = [c for c in self.get_relation_columns(relation_a) if not c[0].lower().startswith('dbt_')]
         return super()._assertTablesEqualSql(relation_a, relation_b, columns=columns)

     def assert_expected(self):
         super().assert_expected()
         self.assertTablesEqual('snapshot_checkall', 'snapshot_expected')


     @property
     def project_config(self):
         return {
             'config-version': 2,
             "data-paths": ['data'],
             "snapshot-paths": ['test-check-col-snapshots-noconfig'],
             "snapshots": {
                 "test": {
                     "target_schema": self.unique_schema(),
                     "unique_key": "id || '-' || first_name",
                     "strategy": "check",
                     "check_cols" : "all",
                     "updated_at": "updated_at",
                 },
             },
             'macro-paths': ['macros'],
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
            'config-version': 2,
            "data-paths": ['data'],
            "snapshot-paths": ['test-check-col-snapshots-bq'],
            'macro-paths': ['macros'],
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

        # check_cols='all' will replace the changed field
        self.run_dbt(['snapshot'])

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
        snapshotted_all_cols = self.get_table_columns(
            database=self.default_database,
            schema=self.unique_schema(),
            table='snapshot_checkall'
        )

        self.assertTrue(len(expected_cols) > 0, "source table does not exist -- bad test")
        self.assertEqual(len(expected_cols), len(snapshotted_cols), "actual and expected column lengths are different")
        self.assertEqual(len(expected_cols), len(snapshotted_all_cols))

        for (expected_col, actual_col) in zip(expected_cols, snapshotted_cols):
            expected_name, expected_type, _ = expected_col
            actual_name, actual_type, _ = actual_col
            self.assertTrue(expected_name is not None)
            self.assertTrue(expected_type is not None)

            self.assertEqual(expected_name, actual_name, "names are different")
            self.assertEqual(expected_type, actual_type, "data types are different")


class TestSnapshotHardDelete(DBTIntegrationTest):
    # These tests uses the same seed data, containing 20 records of which we hard delete the last 10.
    # These deleted records set the dbt_valid_to to time the snapshot was ran.
    NUM_SNAPSHOT_MODELS = 1

    @property
    def schema(self):
        return "simple_snapshot"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        paths = ['test-snapshots-bq']

        return {
            'config-version': 2,
            "data-paths": ['data'],
            "snapshot-paths": paths,
            'macro-paths': ['macros'],
        }

    @use_profile('bigquery')
    def test__bigquery__snapshot_hard_delete(self):
        self.run_sql_file('seed_bq.sql')
        self._test_snapshot_hard_delete()

    def _test_snapshot_hard_delete(self):
        self._snapshot()

        self.assertTablesEqual("snapshot_expected", "snapshot_actual")

        self._invalidated_snapshot_datetime = None
        self._revived_snapshot_datetime = None

        self._delete_records()
        self._snapshot_and_assert_invalidated()
        self._revive_records()
        self._snapshot_and_assert_revived()

    def _snapshot(self):
        begin_snapshot_datetime = datetime.now(pytz.UTC)
        results = self.run_dbt(['snapshot', '--vars', '{invalidate_hard_deletes: true}'])
        self.assertEqual(len(results), self.NUM_SNAPSHOT_MODELS)

        return begin_snapshot_datetime

    def _delete_records(self):
        database = self.adapter.quote(database)

        self.run_sql(
            'delete from {}.{}.seed where id >= 10;'.format(database, self.unique_schema())
        )

    def _snapshot_and_assert_invalidated(self):
        self._invalidated_snapshot_datetime = self._snapshot()

        database = self.adapter.quote(database)

        snapshotted = self.run_sql(
            '''
            select
                id,
                dbt_valid_to
            from {}.{}.snapshot_actual
            order by id
            '''.format(database, self.unique_schema()),
            fetch='all'
        )

        self.assertEqual(len(snapshotted), 20)
        for result in snapshotted[10:]:
            # result is a tuple, the dbt_valid_to column is the latest
            self.assertIsInstance(result[-1], datetime)
            self.assertGreaterEqual(result[-1].astimezone(pytz.UTC), self._invalidated_snapshot_datetime)

    def _revive_records(self):
        database = self.adapter.quote(database)

        revival_timestamp = datetime.now(pytz.UTC).strftime(r'%Y-%m-%d %H:%M:%S')
        self.run_sql(
            '''
            insert into {}.{}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
            (10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '{}'),
            (11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '{}')
            '''.format(database, self.unique_schema(), revival_timestamp, revival_timestamp)
        )

    def _snapshot_and_assert_revived(self):
        self._revived_snapshot_datetime = self._snapshot()

        database = self.adapter.quote(database)

        # records which weren't revived (id != 10, 11)
        invalidated_records = self.run_sql(
            '''
            select
                id,
                dbt_valid_to
            from {}.{}.snapshot_actual
            where dbt_valid_to is not null
            order by id
            '''.format(database, self.unique_schema()),
            fetch='all'
        )

        self.assertEqual(len(invalidated_records), 11)
        for result in invalidated_records:
            # result is a tuple, the dbt_valid_to column is the latest
            self.assertIsInstance(result[1], datetime)
            self.assertGreaterEqual(result[1].astimezone(pytz.UTC), self._invalidated_snapshot_datetime)

        # records which weren't revived (id != 10, 11)
        revived_records = self.run_sql(
            '''
            select
                id,
                dbt_valid_from,
                dbt_valid_to
            from {}.{}.snapshot_actual
            where dbt_valid_to is null
            and id IN (10, 11)
            '''.format(database, self.unique_schema()),
            fetch='all'
        )

        self.assertEqual(len(revived_records), 2)
        for result in revived_records:
            # result is a tuple, the dbt_valid_from is second and dbt_valid_to is latest
            self.assertIsInstance(result[1], datetime)
            # there are milliseconds (part of microseconds in datetime objects) in the
            # invalidated_snapshot_datetime and not in result datetime so set the microseconds to 0
            self.assertGreaterEqual(result[1].astimezone(pytz.UTC), self._invalidated_snapshot_datetime.replace(microsecond=0))
            self.assertIsNone(result[2])
