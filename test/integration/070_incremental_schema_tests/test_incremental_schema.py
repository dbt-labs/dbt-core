from test.integration.base import DBTIntegrationTest, use_profile


class TestIncrementalSchemaChange(DBTIntegrationTest):
    @property
    def schema(self):
        return "test_incremental_schema_070"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "test-paths": ["tests"]
        }

    def run_twice_and_assert(
        self, include, compare_source, compare_target
    ):

        # dbt run (twice)
        run_args = ['run']
        if include:
            run_args.extend(('--models', include))
        results_one = self.run_dbt(run_args)
        results_two = self.run_dbt(run_args)

        self.assertEqual(len(results_one), 3)
        self.assertEqual(len(results_two), 3)
        
        self.assertTablesEqual(compare_source, compare_target)

    def run_incremental_ignore(self):
        select = 'model_a incremental_ignore incremental_ignore_target'
        compare_source = 'incremental_ignore'
        compare_target = 'incremental_ignore_target'
        self.run_twice_and_assert(select, compare_source, compare_target)

    def run_incremental_append_new_columns(self):
        select = 'model_a incremental_append_new_columns incremental_append_new_columns_target'
        compare_source = 'incremental_append_new_columns'
        compare_target = 'incremental_append_new_columns_target'
        self.run_twice_and_assert(select, compare_source, compare_target)

    def run_incremental_sync_all_columns(self):
        select = 'model_a incremental_sync_all_columns incremental_sync_all_columns_target'
        compare_source = 'incremental_sync_all_columns'
        compare_target = 'incremental_sync_all_columns_target'
        self.run_twice_and_assert(select, compare_source, compare_target)
        
    def run_incremental_sync_remove_only(self):
        select = 'model_a incremental_sync_remove_only incremental_sync_remove_only_target'
        compare_source = 'incremental_sync_remove_only'
        compare_target = 'incremental_sync_remove_only_target'
        self.run_twice_and_assert(select, compare_source, compare_target)

    def run_incremental_fail_on_schema_change(self):
        select = 'model_a incremental_fail'
        results_one = self.run_dbt(['run', '--models', select, '--full-refresh'])
        results_two = self.run_dbt(['run', '--models', select], expect_pass = False)
        self.assertIn('Compilation Error', results_two[1].message)

    @use_profile('postgres')
    def test__postgres__run_incremental_ignore(self):
        self.run_incremental_ignore()

    @use_profile('postgres')
    def test__postgres__run_incremental_append_new_columns(self):
        self.run_incremental_append_new_columns()

    @use_profile('postgres')
    def test__postgres__run_incremental_sync_all_columns(self):
        self.run_incremental_sync_all_columns()
        self.run_incremental_sync_remove_only()

    @use_profile('postgres')
    def test__postgres__run_incremental_fail_on_schema_change(self):
        self.run_incremental_fail_on_schema_change()
