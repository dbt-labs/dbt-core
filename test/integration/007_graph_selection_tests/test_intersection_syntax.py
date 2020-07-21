from test.integration.base import DBTIntegrationTest, use_profile
import yaml


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
            - name: same_intersection
              definition:
                intersection:
                  - fqn: users
                  - fqn:users
            - name: tags_intersection
              definition:
                intersection:
                  - tag: bi
                  - tag: users
            - name: triple_descending
              definition:
                intersection:
                  - fqn: "*"
                  - tag: bi
                  - tag: users
            - name: triple_ascending
              definition:
                intersection:
                  - tag: users
                  - tag: bi
                  - fqn: "*"
            - name: intersection_with_exclusion
              definition:
                intersection:
                  - method: fqn
                    value: users_rollup_dependency
                    parents: true
                  - method: fqn
                    value: users
                    children: true
                  - exclude:
                    - users_rollup_dependency
            - name: intersection_exclude_intersection
              definition:
                intersection:
                  - tag:bi
                  - "@users"
                  - exclude:
                      - intersection:
                        - tag:bi
                        - method: fqn
                          value: users_rollup
                          children: true
            - name: intersection_exclude_intersection_lack
              definition:
                intersection:
                  - tag:bi
                  - "@users"
                  - exclude:
                      - intersection:
                        - method: fqn
                          value: emails
                          children_parents: true
                        - method: fqn
                          value: emails_alt
                          children_parents: true
        ''')

    def _verify_selected_users(self, results):
        # users
        self.assertEqual(len(results), 1)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__same_model_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users,users'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__same_model_intersection_selectors(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--selector', 'same_intersection'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__tags_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:bi,tag:users'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__tags_intersection_selectors(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--selector', 'tags_intersection'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__intersection_triple_descending(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '*,tag:bi,tag:users'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__intersection_triple_descending_schema(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '*,tag:bi,tag:users'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__intersection_triple_descending_schema_selectors(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--selector', 'triple_descending'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__intersection_triple_ascending(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:users,tag:bi,*'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__intersection_triple_ascending_schema_selectors(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--selector', 'triple_ascending'])
        self._verify_selected_users(results)

    def _verify_selected_users_and_rollup(self, results):
        # users, users_rollup
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertIn('users', created_models)
        self.assertIn('users_rollup', created_models)
        self.assertNotIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @use_profile('postgres')
    def test__postgres__intersection_with_exclusion(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '+users_rollup_dependency,users+', '--exclude', 'users_rollup_dependency'])
        self._verify_selected_users_and_rollup(results)

    @use_profile('postgres')
    def test__postgres__intersection_with_exclusion_selectors(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--selector', 'intersection_with_exclusion'])
        self._verify_selected_users_and_rollup(results)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             'tag:bi,users_rollup+'])
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection_selectors(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--selector', 'intersection_exclude_intersection']
        )
        self._verify_selected_users(results)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection_lack(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             '@emails,@emails_alt'])
        self._verify_selected_users_and_rollup(results)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection_lack_selector(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--selector', 'intersection_exclude_intersection_lack'])
        self._verify_selected_users_and_rollup(results)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_triple_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             '*,tag:bi,users_rollup'])
        self._verify_selected_users(results)

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
