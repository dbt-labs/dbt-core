from test.integration.base import DBTIntegrationTest, use_profile


class TestAliases(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros'],
            "models": {
                "test": {
                    "alias_in_project": {
                        "alias": 'project_alias',
                    },
                    "alias_in_project_with_override": {
                        "alias": 'project_alias',
                    },
                }
            }
        }

    @use_profile('postgres')
    def test__alias_model_name_postgres(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 4)
        self.run_dbt(['test'])


class TestAliasErrors(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "models-dupe"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros'],
        }

    @use_profile('postgres')
    def test__postgres_alias_dupe_throws_exception(self):
        message = ".*identical database representation.*"
        with self.assertRaisesRegex(Exception, message):
            self.run_dbt(['run'])


class TestSameAliasDifferentSchemas(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "models-dupe-custom-schema"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros'],
        }

    def setUp(self):
        super().setUp()
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.unique_schema() + '_schema_a')
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.unique_schema() + '_schema_b')
        )

    @use_profile('postgres')
    def test__postgres_same_alias_succeeds_in_different_schemas(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 3)
        res = self.run_dbt(['test'])

        # Make extra sure the tests ran
        self.assertTrue(len(res) > 0)


class TestSameAliasDifferentDatabases(DBTIntegrationTest):
    setup_alternate_db = True

    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "models-dupe-custom-database"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros'],
            'models': {
                'test': {
                    'alias': 'duped_alias',
                    'model_b': {
                        'database': self.alternative_database,
                    },
                },
            }
        }
