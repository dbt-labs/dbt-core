from test.integration.base import DBTIntegrationTest, use_profile

import os


class BaseOverrideDatabase(DBTIntegrationTest):
    setup_alternate_db = True
    @property
    def schema(self):
        return "override_database_040"

    @property
    def models(self):
        return "models"

    @property
    def alternative_database(self):
        if self.adapter_type == 'snowflake':
            return os.getenv('SNOWFLAKE_TEST_DATABASE')
        else:
            return super().alternative_database

    def snowflake_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'snowflake',
                        'threads': 4,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': os.getenv('SNOWFLAKE_TEST_USER'),
                        'password': os.getenv('SNOWFLAKE_TEST_PASSWORD'),
                        'database': os.getenv('SNOWFLAKE_TEST_QUOTED_DATABASE'),
                        'schema': self.unique_schema(),
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    },
                    'noaccess': {
                        'type': 'snowflake',
                        'threads': 4,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': 'noaccess',
                        'password': 'password',
                        'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                        'schema': self.unique_schema(),
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    }
                },
                'target': 'default2'
            }
        }

    @property
    def project_config(self):
        return {
            "data-paths": ['data'],
            'models': {
                'vars': {
                    'alternate_db': self.alternative_database,
                },
            },
            'quoting': {
                'database': True,
            }
        }

    def run_dbt_notstrict(self, args):
        return self.run_dbt(args, strict=False)


class TestModelOverride(BaseOverrideDatabase):
    def run_database_override(self):
        if self.adapter_type == 'snowflake':
            func = lambda x: x.upper()
        else:
            func = lambda x: x

        self.run_dbt_notstrict(['seed'])

        self.assertEqual(len(self.run_dbt_notstrict(['run'])), 4)
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

    @use_profile('snowflake')
    def test_snowflake_database_override(self):
        self.run_database_override()


class TestProjectModelOverride(BaseOverrideDatabase):
    def run_database_override(self):
        if self.adapter_type == 'snowflake':
            func = lambda x: x.upper()
        else:
            func = lambda x: x

        self.use_default_project({
            'models': {
                'vars': {
                    'alternate_db': self.alternative_database,
                },
                'database': self.alternative_database,
                'test': {
                    'subfolder': {
                        'database': self.default_database,
                    },
                },
            }
        })
        self.run_dbt_notstrict(['seed'])

        self.assertEqual(len(self.run_dbt_notstrict(['run'])), 4)
        self.assertManyRelationsEqual([
            (func('seed'), self.unique_schema(), self.default_database),
            (func('view_2'), self.unique_schema(), self.alternative_database),
            (func('view_1'), self.unique_schema(), self.alternative_database),
            (func('view_3'), self.unique_schema(), self.default_database),
            (func('view_4'), self.unique_schema(), self.alternative_database),
        ])

    @use_profile('bigquery')
    def test_bigquery_database_override(self):
        self.run_database_override()

    @use_profile('snowflake')
    def test_snowflake_database_override(self):
        self.run_database_override()


class TestProjectSeedOverride(BaseOverrideDatabase):
    def run_database_override(self):
        if self.adapter_type == 'snowflake':
            func = lambda x: x.upper()
        else:
            func = lambda x: x

        self.use_default_project({
            'seeds': {'database': self.alternative_database}
        })
        self.run_dbt_notstrict(['seed'])

        self.assertEqual(len(self.run_dbt_notstrict(['run'])), 4)
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

    @use_profile('snowflake')
    def test_snowflake_database_override(self):
        self.run_database_override()
