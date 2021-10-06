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
    def project_config(self):
        return {
            'config-version': 2,
            "models": {
                "test": {
                    "users": {
                        "tags": "specified_as_string"
                    },
                    "users_rollup": {
                        "tags": ["specified_in_project"],
                    }
                }
            }
        }

    @property
    def selectors_config(self):
        return yaml.safe_load('''
            selectors:
              - name: tag_specified_as_string_str
                definition: tag:specified_as_string
              - name: tag_specified_as_string_dict
                definition:
                  method: tag
                  value: specified_as_string
              - name: tag_specified_in_project_children_str
                definition: +tag:specified_in_project+
              - name: tag_specified_in_project_children_dict
                definition:
                  method: tag
                  value: specified_in_project
                  parents: true
                  children: true
              - name: tagged-bi
                definition:
                  method: tag
                  value: bi
              - name: user_tagged_childrens_parents
                definition:
                  method: tag
                  value: users
                  childrens_parents: true
              - name: base_ephemerals
                definition:
                  union:
                    - tag: base
                    - method: config.materialized
                      value: ephemeral
              - name: warn-severity
                definition:
                    config.severity: warn
              - name: roundabout-everything
                definition:
                    union:
                        - "@tag:users"
                        - intersection:
                            - tag: base
                            - config.materialized: ephemeral
        ''')

    def setUp(self):
        super().setUp()
        self.run_sql_file("seed.sql")

    def _verify_select_tag(self, results):
        self.assertEqual(len(results), 1)

        models_run = [r.node.name for r in results]
        self.assertTrue('users' in models_run)

    @use_profile('postgres')
    def test__postgres__select_tag(self):
        results = self.run_dbt(['run', '--models', 'tag:specified_as_string'])
        self._verify_select_tag(results)

    @use_profile('postgres')
    def test__postgres__select_tag_selector_str(self):
        results = self.run_dbt(['run', '--selector', 'tag_specified_as_string_str'])
        self._verify_select_tag(results)

    @use_profile('postgres')
    def test__postgres__select_tag_selector_dict(self):
        results = self.run_dbt(['run', '--selector', 'tag_specified_as_string_dict'])
        self._verify_select_tag(results)

    def _verify_select_tag_and_children(self, results):
        self.assertEqual(len(results), 3)

        models_run = [r.node.name for r in results]
        self.assertTrue('users' in models_run)
        self.assertTrue('users_rollup' in models_run)

    @use_profile('postgres')
    def test__postgres__select_tag_and_children(self):
        results = self.run_dbt(['run', '--models', '+tag:specified_in_project+'])
        self._verify_select_tag_and_children(results)

    @use_profile('postgres')
    def test__postgres__select_tag_and_children_selector_str(self):
        results = self.run_dbt(['run', '--selector', 'tag_specified_in_project_children_str'])
        self._verify_select_tag_and_children(results)

    @use_profile('postgres')
    def test__postgres__select_tag_and_children_selector_dict(self):
        results = self.run_dbt(['run', '--selector', 'tag_specified_in_project_children_dict'])
        self._verify_select_tag_and_children(results)

    # check that model configs aren't squashed by project configs
    def _verify_select_bi(self, results):
        self.assertEqual(len(results), 2)

        models_run = [r.node.name for r in results]
        self.assertTrue('users' in models_run)
        self.assertTrue('users_rollup' in models_run)

    @use_profile('postgres')
    def test__postgres__select_tag_in_model_with_project_config(self):
        results = self.run_dbt(['run', '--models', 'tag:bi'])
        self._verify_select_bi(results)

    @use_profile('postgres')
    def test__postgres__select_tag_in_model_with_project_config_selector(self):
        results = self.run_dbt(['run', '--selector', 'tagged-bi'])
        self._verify_select_bi(results)

    # check that model configs aren't squashed by project configs
    @use_profile('postgres')
    def test__postgres__select_tag_in_model_with_project_config_parents_children(self):
        results = self.run_dbt(['run', '--models', '@tag:users'])
        self.assertEqual(len(results), 4)

        models_run = set(r.node.name for r in results)
        self.assertEqual(
            {'users', 'users_rollup', 'emails_alt', 'users_rollup_dependency'},
            models_run
        )

        # just the users/users_rollup tests
        results = self.run_dbt(['test', '--models', '@tag:users'])
        self.assertEqual(len(results), 2)
        assert sorted(r.node.name for r in results) == ['unique_users_id', 'unique_users_rollup_gender']
        # just the email test
        results = self.run_dbt(['test', '--models', 'tag:base,config.materialized:ephemeral'])
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'not_null_emails_email'
        # also just the email test
        results = self.run_dbt(['test', '--models', 'config.severity:warn'])
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'not_null_emails_email'
        # all 3 tests
        results = self.run_dbt(['test', '--models', '@tag:users tag:base,config.materialized:ephemeral'])
        self.assertEqual(len(results), 3)
        assert sorted(r.node.name for r in results) == ['not_null_emails_email', 'unique_users_id', 'unique_users_rollup_gender']


    @use_profile('postgres')
    def test__postgres__select_tag_in_model_with_project_config_parents_children_selectors(self):
        results = self.run_dbt(['run', '--selector', 'user_tagged_childrens_parents'])
        self.assertEqual(len(results), 4)

        models_run = set(r.node.name for r in results)
        self.assertEqual(
            {'users', 'users_rollup', 'emails_alt', 'users_rollup_dependency'},
            models_run
        )

        # just the users/users_rollup tests
        results = self.run_dbt(['test', '--selector', 'user_tagged_childrens_parents'])
        self.assertEqual(len(results), 2)
        assert sorted(r.node.name for r in results) == ['unique_users_id', 'unique_users_rollup_gender']
        # just the email test
        results = self.run_dbt(['test', '--selector', 'base_ephemerals'])
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'not_null_emails_email'

        # also just the email test
        results = self.run_dbt(['test', '--selector', 'warn-severity'])
        self.assertEqual(len(results), 1)
        assert results[0].node.name == 'not_null_emails_email'
        # all 3 tests
        results = self.run_dbt(['test', '--selector', 'roundabout-everything'])
        self.assertEqual(len(results), 3)
        assert sorted(r.node.name for r in results) == ['not_null_emails_email', 'unique_users_id', 'unique_users_rollup_gender']
