from test.integration.base import DBTIntegrationTest, use_profile


class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test__postgres__same_model_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users,users'])
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__tags_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:bi,tag:users'])
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_triple_descending(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '*,tag:bi,tag:users'])
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_triple_ascending(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:users,tag:bi,*'])
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_with_exclusion(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '+users_rollup_dependency,users+', '--exclude', 'users_rollup_dependency'])
        # users, users_rollup
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             'tag:bi,users_rollup+'])
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection_lack(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             '@emails,@emails_alt'])
        # users, users_rollup
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)


    @use_profile('postgres')
    def test__postgres__intersection_exclude_triple_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             '*,tag:bi,users_rollup'])
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', 'emails_alt'])
        # users, users_rollup, emails_alt
        self.assertEqual(len(results), 3)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '@emails_alt,emails_alt'])
        # users, users_rollup, emails_alt
        self.assertEqual(len(results), 3)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat_exclude(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', 'emails_alt', '--exclude', 'users_rollup']
        )
        # users, emails_alt
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat_exclude_concat(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', 'emails_alt,@users',
             '--exclude', 'users_rollup_dependency', 'users_rollup'])
        # users, emails_alt
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)


    @use_profile('postgres')
    def test__postgres__intersection_concat_exclude_intersection_concat(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', 'emails_alt,@users',
             '--exclude', '@users,users_rollup_dependency', '@users,users_rollup'])
        # users, emails_alt
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)
