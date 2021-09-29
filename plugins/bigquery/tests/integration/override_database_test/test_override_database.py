from tests.integration.base import DBTIntegrationTest, use_profile

import os


class BaseOverrideDatabase(DBTIntegrationTest):
    setup_alternate_db = True
    @property
    def schema(self):
        return "override_database"

    @property
    def models(self):
        return "models"

    @property
    def alternative_database(self):
        return super().alternative_database

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }


class TestModelOverride(BaseOverrideDatabase):
    def run_database_override(self):
        func = lambda x: x

        self.run_dbt(['seed'])

        self.assertEqual(len(self.run_dbt(['run'])), 4)
        self.assertManyRelationsEqual([
            (func('seed'), self.unique_schema(), self.default_database),
            (func('view_2'), self.unique_schema(), self.alternative_database),
            (func('view_1'), self.unique_schema(), self.default_database),
            (func('view_3'), self.unique_schema(), self.default_database),
            (func('view_4'), self.unique_schema(), self.alternative_database),
        ])

    @use_profile('bigquery')
    def test_bigquery_database_override(self):
        self.run_database_override()


class BaseTestProjectModelOverride(BaseOverrideDatabase):
    # this is janky, but I really want to access self.default_database in
    # project_config
    @property
    def default_database(self):
        target = self._profile_config['test']['target']
        profile = self._profile_config['test']['outputs'][target]
        for key in ['database', 'project', 'dbname']:
            if key in profile:
                database = profile[key]
                return database
        assert False, 'No profile database found!'

    def run_database_override(self):
        self.run_dbt(['seed'])
        self.assertEqual(len(self.run_dbt(['run'])), 4)
        self.assertExpectedRelations()

    def assertExpectedRelations(self):
        func = lambda x: x

        self.assertManyRelationsEqual([
            (func('seed'), self.unique_schema(), self.default_database),
            (func('view_2'), self.unique_schema(), self.alternative_database),
            (func('view_1'), self.unique_schema(), self.alternative_database),
            (func('view_3'), self.unique_schema(), self.default_database),
            (func('view_4'), self.unique_schema(), self.alternative_database),
        ])


class TestProjectModelOverride(BaseTestProjectModelOverride):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'models': {
                'database': self.alternative_database,
                'test': {
                    'subfolder': {
                        'database': self.default_database,
                    }
                }
            },
            'data-paths': ['data'],
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile('bigquery')
    def test_bigquery_database_override(self):
        self.run_database_override()


class TestProjectModelAliasOverride(BaseTestProjectModelOverride):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'models': {
                'project': self.alternative_database,
                'test': {
                    'subfolder': {
                        'project': self.default_database,
                    }
                }
            },
            'data-paths': ['data'],
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile('bigquery')
    def test_bigquery_project_override(self):
        self.run_database_override()


class TestProjectSeedOverride(BaseOverrideDatabase):
    def run_database_override(self):
        func = lambda x: x

        self.use_default_project({
            'config-version': 2,
            'seeds': {
                'database': self.alternative_database
            },
        })
        self.run_dbt(['seed'])

        self.assertEqual(len(self.run_dbt(['run'])), 4)
        self.assertManyRelationsEqual([
            (func('seed'), self.unique_schema(), self.alternative_database),
            (func('view_2'), self.unique_schema(), self.alternative_database),
            (func('view_1'), self.unique_schema(), self.default_database),
            (func('view_3'), self.unique_schema(), self.default_database),
            (func('view_4'), self.unique_schema(), self.alternative_database),
        ])

    @use_profile('bigquery')
    def test_bigquery_database_override(self):
        self.run_database_override()
