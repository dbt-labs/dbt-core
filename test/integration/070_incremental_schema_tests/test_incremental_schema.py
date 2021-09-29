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

    def list_tests_and_assert(self, include, exclude, expected_tests):
        list_args = ['ls', '--resource-type', 'test']
        if include:
            list_args.extend(('--select', include))
        if exclude:
            list_args.extend(('--exclude', exclude))
        listed = self.run_dbt(list_args)
        print(listed)
        assert len(listed) == len(expected_tests)
        
        test_names = [name.split('.')[-1] for name in listed]
        assert sorted(test_names) == sorted(expected_tests)

    def run_tests_and_assert(
        self, include, exclude, expected_tests, compare_source, compare_target
    ):  

        run_args = ['run']
        if include:
            run_args.extend(('--models', include))
        results_one = self.run_dbt(run_args)
        results_two = self.run_dbt(run_args)

        self.assertEqual(len(results_one), 3)
        self.assertEqual(len(results_two), 3)

        test_args = ['test']
        if include:
            test_args.extend(('--models', include))
        if exclude:
            test_args.extend(('--exclude', exclude))
        
        results = self.run_dbt(test_args)
        tests_run = [r.node.name for r in results]
        assert len(tests_run) == len(expected_tests)
        assert sorted(tests_run) == sorted(expected_tests)
        self.assertTablesEqual(compare_source, compare_target)

    def run_incremental_ignore(self):
        select = 'model_a incremental_ignore incremental_ignore_target'
        compare_source = 'incremental_ignore'
        compare_target = 'incremental_ignore_target'
        exclude = None
        expected = [
            'select_from_a',
            'select_from_incremental_ignore',
            'select_from_incremental_ignore_target',
            'unique_model_a_id',
            'unique_incremental_ignore_id',
            'unique_incremental_ignore_target_id'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected, compare_source, compare_target)

    def run_incremental_append_new_columns(self):
        select = 'model_a incremental_append_new_columns incremental_append_new_columns_target'
        compare_source = 'incremental_append_new_columns'
        compare_target = 'incremental_append_new_columns_target'
        exclude = None
        expected = [
            'select_from_a',
            'select_from_incremental_append_new_columns',
            'select_from_incremental_append_new_columns_target',
            'unique_model_a_id',
            'unique_incremental_append_new_columns_id',
            'unique_incremental_append_new_columns_target_id'
        ]
        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected, compare_source, compare_target)

    def run_incremental_sync_all_columns(self):
        select = 'model_a incremental_sync_all_columns incremental_sync_all_columns_target'
        compare_source = 'incremental_sync_all_columns'
        compare_target = 'incremental_sync_all_columns_target'
        exclude = None
        expected = [
            'select_from_a',
            'select_from_incremental_sync_all_columns',
            'select_from_incremental_sync_all_columns_target',
            'unique_model_a_id',
            'unique_incremental_sync_all_columns_id',
            'unique_incremental_sync_all_columns_target_id'
        ]
        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected, compare_source, compare_target)

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

    @use_profile('postgres')
    def test__postgres__run_incremental_fail_on_schema_change(self):
        self.run_incremental_fail_on_schema_change()
