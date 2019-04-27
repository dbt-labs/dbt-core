from test.integration.base import DBTIntegrationTest, use_profile


class BaseOverrideDatabase(DBTIntegrationTest):
    setup_alternate_db = True
    @property
    def schema(self):
        return "override_database_040"

    @property
    def models(self):
        return "test/integration/040_override_database_test/models"

    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/040_override_database_test/data'],
            'models': {
                'vars': {
                    'alternate_db': self.alternative_database,
                },
            }
        }


class TestModelOverride(BaseOverrideDatabase):
    def run_database_override(self):
        if self.adapter_type == 'snowflake':
            func = lambda x: x.upper()
        else:
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
        self.run_dbt(['seed'])

        self.assertEqual(len(self.run_dbt(['run'])), 4)
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

    @use_profile('snowflake')
    def test_snowflake_database_override(self):
        self.run_database_override()
