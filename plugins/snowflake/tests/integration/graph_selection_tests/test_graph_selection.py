from tests.integration.base import DBTIntegrationTest, use_profile
import yaml
import json
import os

class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests"

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

    @use_profile('snowflake')
    def test__snowflake__specific_model(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', 'users'])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("SEED", "USERS")
        created_models = self.get_models_in_schema()
        self.assertFalse('USERS_ROLLUP' in created_models)
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)
        self.assert_correct_schemas()

    @use_profile('snowflake')
    def test__snowflake__specific_model_and_children(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', 'users+'])
        self.assertEqual(len(results),  4)

        self.assertManyTablesEqual(
            ["SEED", "USERS"],
            ["SUMMARY_EXPECTED", "USERS_ROLLUP"]
        )
        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)

    @use_profile('snowflake')
    def test__snowflake__specific_model_and_parents(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--select', '+users_rollup'])
        self.assertEqual(len(results),  2)

        self.assertManyTablesEqual(
            ["SEED", "USERS"],
            ["SUMMARY_EXPECTED", "USERS_ROLLUP"]
        )

        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)

    @use_profile('snowflake')
    def test__snowflake__specific_model_with_exclusion(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--select', '+users_rollup', '--exclude', 'users_rollup']
        )
        self.assertEqual(len(results),  1)

        self.assertManyTablesEqual(["SEED", "USERS"])
        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('USERS_ROLLUP' in created_models)
        self.assertFalse('EMAILS' in created_models)

    @use_profile('snowflake')
    def test__snowflake__skip_intermediate(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt(['run', '--select', '@models/users.sql'])
        # base_users, emails, users_rollup, users_rollup_dependency
        self.assertEqual(len(results), 4)

        # now re-run, skipping users_rollup
        results = self.run_dbt(['run', '--select', '@users', '--exclude', 'users_rollup'])
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
