from test.integration.base import DBTIntegrationTest, use_profile

class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "models"

    def assert_correct_schemas(self):
        with self.get_connection():
            exists = self.adapter.check_schema_exists(
                self.default_database,
                self.unique_schema()
            )
            self.assertTrue(exists)

            schema = self.unique_schema()+'_and_then'
            exists = self.adapter.check_schema_exists(
                self.default_database,
                schema
            )
            self.assertFalse(exists)

    @use_profile('postgres')
    def test__postgres__specific_model(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__tags(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:bi'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertTrue('users' in created_models)
        self.assertTrue('users_rollup' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__tags_and_children(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:base+'])
        self.assertEqual(len(results), 4)

        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertIn('emails_alt', created_models)
        self.assertTrue('users_rollup' in created_models)
        self.assertTrue('users' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__tags_and_children_limited(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:base+2'])
        self.assertEqual(len(results), 3)

        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertIn('emails_alt', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup_dependency', created_models)
        self.assert_correct_schemas()

    @use_profile('snowflake')
    def test__snowflake__specific_model(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users'])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("SEED", "USERS")
        created_models = self.get_models_in_schema()
        self.assertFalse('USERS_ROLLUP' in created_models)
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__specific_model_and_children(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users+'])
        self.assertEqual(len(results),  4)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assert_correct_schemas()

    @use_profile('snowflake')
    def test__snowflake__specific_model_and_children(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users+'])
        self.assertEqual(len(results),  4)

        self.assertManyTablesEqual(
            ["SEED", "USERS"],
            ["SUMMARY_EXPECTED", "USERS_ROLLUP"]
        )
        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)

    @use_profile('postgres')
    def test__postgres__specific_model_and_children_limited(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users+1'])
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assertNotIn('users_rollup_dependency', created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__specific_model_and_parents(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '+users_rollup'])
        self.assertEqual(len(results),  2)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @use_profile('snowflake')
    def test__snowflake__specific_model_and_parents(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '+users_rollup'])
        self.assertEqual(len(results),  2)

        self.assertManyTablesEqual(
            ["SEED", "USERS"],
            ["SUMMARY_EXPECTED", "USERS_ROLLUP"]
        )

        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)

    @use_profile('postgres')
    def test__postgres__specific_model_and_parents_limited(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '1+users_rollup'])
        self.assertEqual(len(results), 2)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__specific_model_with_exclusion(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', '+users_rollup', '--exclude', 'models/users_rollup.sql']
        )
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @use_profile('snowflake')
    def test__snowflake__specific_model_with_exclusion(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', '+users_rollup', '--exclude', 'users_rollup']
        )
        self.assertEqual(len(results),  1)

        self.assertManyTablesEqual(["SEED", "USERS"])
        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('USERS_ROLLUP' in created_models)
        self.assertFalse('EMAILS' in created_models)

    @use_profile('postgres')
    def test__postgres__locally_qualified_name(self):
        results = self.run_dbt(['run', '--models', 'test.subdir'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assertIn('subdir', created_models)
        self.assertIn('nested_users', created_models)
        self.assert_correct_schemas()

        results = self.run_dbt(['run', '--models', 'models/test/subdir*'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assertIn('subdir', created_models)
        self.assertIn('nested_users', created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__childrens_parents(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--models', '@base_users'])
        self.assertEqual(len(results), 4)

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

        results = self.run_dbt(
            ['test', '--models', 'test_name:not_null'],
        )
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'not_null_emails_email'

    @use_profile('postgres')
    def test__postgres__more_childrens_parents(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--models', '@users'])
        # users, emails_alt, users_rollup, users_rollup_dependency, but not base_users (ephemeral)
        self.assertEqual(len(results), 4)

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

        results = self.run_dbt(
            ['test', '--models', 'test_name:unique'],
        )
        self.assertEqual(len(results), 2)
        assert sorted([r.node.name for r in results]) == ['unique_users_id', 'unique_users_rollup_gender']


    @use_profile('snowflake')
    def test__snowflake__skip_intermediate(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--models', '@models/users.sql'])
        # base_users, emails, users_rollup, users_rollup_dependency
        self.assertEqual(len(results), 4)

        # now re-run, skipping users_rollup
        results = self.run_dbt(['run', '--models', '@users', '--exclude', 'users_rollup'])
        self.assertEqual(len(results), 3)

        # make sure that users_rollup_dependency and users don't interleave
        users = [r for r in results if r.node.name == 'users'][0]
        dep = [r for r in results if r.node.name == 'users_rollup_dependency'][0]
        user_last_end = users.timing[1].completed_at
        dep_first_start = dep.timing[0].started_at
        self.assertTrue(
            user_last_end <= dep_first_start,
            'dependency started before its transitive parent ({} > {})'.format(user_last_end, dep_first_start)
        )

    @use_profile('postgres')
    def test__postgres__concat(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--models', '@emails_alt', 'users_rollup'])
        # users, emails_alt, users_rollup
        self.assertEqual(len(results), 3)

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__concat_exclude(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--models', '@emails_alt', 'users_rollup', '--exclude', 'emails_alt'])
        # users, users_rollup
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__concat_exclude_concat(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(
            ['run', '--models', '@emails_alt', 'users_rollup', '--exclude', 'emails_alt', 'users_rollup']
        )
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()

        self.assertIn('users', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

        results = self.run_dbt(
            ['test', '--models', '@emails_alt', 'users_rollup', '--exclude', 'emails_alt', 'users_rollup']
        )
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'unique_users_id'

