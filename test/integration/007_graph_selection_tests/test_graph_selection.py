from test.integration.base import DBTIntegrationTest, use_profile
import yaml
import json
import os

class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "models"

    @property
    def selectors_config(self):
        return yaml.safe_load('''
            selectors:
            - name: bi_selector
              description: This is a BI selector
              definition:
                method: tag
                value: bi
        ''')


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

        results = self.run_dbt(['run', '--select', 'users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('alternative.users' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__tags(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--selector', 'bi_selector'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertFalse('alternative.users' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertTrue('users' in created_models)
        self.assertTrue('users_rollup' in created_models)
        self.assert_correct_schemas()
        self.assertTrue(os.path.exists('./target/manifest.json'))
        with open('./target/manifest.json') as fp:
            manifest = json.load(fp)
        self.assertTrue('selectors' in manifest)



    @use_profile('postgres')
    def test__postgres__tags_and_children(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', 'tag:base+'])
        self.assertEqual(len(results), 5)

        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertIn('emails_alt', created_models)
        self.assertTrue('users_rollup' in created_models)
        self.assertTrue('users' in created_models)
        self.assertTrue('alternative.users' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__tags_and_children_limited(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', 'tag:base+2'])
        self.assertEqual(len(results), 4)

        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertIn('emails_alt', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('alternative.users', created_models)
        self.assertNotIn('users_rollup_dependency', created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__specific_model_and_children(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', 'users+'])
        self.assertEqual(len(results),  4)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('alternative.users', created_models)
        self.assertNotIn('emails', created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__specific_model_and_children_limited(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', 'users+1'])
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

        results = self.run_dbt(['run', '--select', '+users_rollup'])
        self.assertEqual(len(results),  2)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__specific_model_and_parents_limited(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', '1+users_rollup'])
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
            ['run', '--select', '+users_rollup', '--exclude', 'models/users_rollup.sql']
        )
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__locally_qualified_name(self):
        results = self.run_dbt(['run', '--select', 'test.subdir'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assertIn('subdir', created_models)
        self.assertIn('nested_users', created_models)
        self.assert_correct_schemas()

        results = self.run_dbt(['run', '--select', 'models/test/subdir*'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assertIn('subdir', created_models)
        self.assertIn('nested_users', created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__locally_qualified_name_model_with_dots(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--select', 'alternative.users'])
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('alternative.users', created_models)
        self.assert_correct_schemas()

        results = self.run_dbt(['run', '--select', 'models/alternative.*'])
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('alternative.users', created_models)
        self.assert_correct_schemas()

    @use_profile('postgres')
    def test__postgres__childrens_parents(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--select', '@base_users'])
        self.assertEqual(len(results), 5)

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertIn('alternative.users', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

        results = self.run_dbt(
            ['test', '--select', 'test_name:not_null'],
        )
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'not_null_emails_email'

    @use_profile('postgres')
    def test__postgres__more_childrens_parents(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--select', '@users'])
        # users, emails_alt, users_rollup, users_rollup_dependency, but not base_users (ephemeral)
        self.assertEqual(len(results), 4)

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

        results = self.run_dbt(
            ['test', '--select', 'test_name:unique'],
        )
        self.assertEqual(len(results), 2)
        assert sorted([r.node.name for r in results]) == ['unique_users_id', 'unique_users_rollup_gender']

    @use_profile('postgres')
    def test__postgres__concat(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--select', '@emails_alt', 'users_rollup'])
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
        results = self.run_dbt(['run', '--select', '@emails_alt', 'users_rollup', '--exclude', 'emails_alt'])
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
            ['run', '--select', '@emails_alt', 'users_rollup', '--exclude', 'emails_alt', 'users_rollup']
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
            ['test', '--select', '@emails_alt', 'users_rollup', '--exclude', 'emails_alt', 'users_rollup']
        )
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'unique_users_id'

    @use_profile('postgres')
    def test__postgres__exposure_parents(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['ls', '--select', '+exposure:seed_ml_exposure'])
        assert len(results) == 2
        assert sorted(results) == ['exposure:test.seed_ml_exposure', 'source:test.raw.seed']

        results = self.run_dbt(['ls', '--select', '1+exposure:user_exposure'])
        assert len(results) == 5
        assert sorted(results) == ['exposure:test.user_exposure', 'test.unique_users_id', 
            'test.unique_users_rollup_gender', 'test.users', 'test.users_rollup']

        results = self.run_dbt(['run', '-m', '+exposure:user_exposure'])
        # users, users_rollup
        assert len(results) == 2

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)
