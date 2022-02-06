from test.integration.base import DBTIntegrationTest, use_profile


class TestCustomSchema(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v2_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.xf_schema())
        )

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "models"

    def v2_schema(self):
        return f"{self.unique_schema()}_custom"

    def xf_schema(self):
        return f"{self.unique_schema()}_test"

    @use_profile('postgres')
    def test__postgres__custom_schema_no_prefix(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema()

        self.assertTablesEqual("seed", "view_1")
        self.assertTablesEqual("seed", "view_2", schema, self.v2_schema())
        self.assertTablesEqual("agg", "view_3", schema, self.xf_schema())


class TestCustomProjectSchemaWithPrefix(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v1_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v2_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.xf_schema())
        )

    @property
    def schema(self):
        return "custom_prefix_024"

    @property
    def models(self):
        return "models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'my-target': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema(),
                    }
                },
                'target': 'my-target'
            }
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "models": {
                "schema": "dbt_test"
            },
        }

    def v1_schema(self):
        return f"{self.unique_schema()}_dbt_test"

    def v2_schema(self):
        return f"{self.unique_schema()}_custom"

    def xf_schema(self):
        return f"{self.unique_schema()}_test"

    def _list_schemas(self):
        with self.get_connection():
            return set(self.adapter.list_schemas(self.default_database))

    def assert_schemas_created(self, expected):
        assert self._list_schemas().intersection(expected) == expected

    def assert_schemas_not_created(self, expected):
        assert not self._list_schemas().intersection(expected)

    @use_profile('postgres')
    def test__postgres__custom_schema_with_prefix(self):
        schema = self.unique_schema()
        new_schemas = {self.v1_schema(), self.v2_schema(), self.xf_schema()}

        self.assert_schemas_not_created(new_schemas)

        self.run_sql_file("seed.sql")

        self.run_dbt(['ls'])
        self.assert_schemas_not_created(new_schemas)
        self.run_dbt(['compile'])
        self.assert_schemas_not_created(new_schemas)

        results = self.run_dbt()
        self.assertEqual(len(results), 3)
        self.assert_schemas_created(new_schemas)

        self.assertTablesEqual("seed", "view_1", schema, self.v1_schema())
        self.assertTablesEqual("seed", "view_2", schema, self.v2_schema())
        self.assertTablesEqual("agg", "view_3", schema, self.xf_schema())


class TestCustomSchemaWithCustomMacro(DBTIntegrationTest):
    def setUp(self):
        super().setUp()

        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v1_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v2_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.xf_schema())
        )

    @property
    def schema(self):
        return "custom_macro_024"

    @property
    def models(self):
        return "models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'prod': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema(),
                    }
                },
                'target': 'prod'
            }
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['custom-macros'],
            'models': {
                'schema': 'dbt_test',
            }
        }

    def v1_schema(self):
        return f"dbt_test_{self.unique_schema()}_macro"

    def v2_schema(self):
        return f"custom_{self.unique_schema()}_macro"

    def xf_schema(self):
        return f"test_{self.unique_schema()}_macro"

    @use_profile('postgres')
    def test__postgres__custom_schema_from_macro(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema()

        self.assertTablesEqual("seed", "view_1", schema, self.v1_schema())
        self.assertTablesEqual("seed", "view_2", schema, self.v2_schema())
        self.assertTablesEqual("agg", "view_3", schema, self.xf_schema())


class TestCustomSchemaDispatchedPackageMacro(TestCustomSchemaWithCustomMacro):
    # test the same functionality as above, but this time,
    # dbt.generate_schema_name will dispatch to a default__ macro
    # from an installed package, per dispatch config search_order

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "dispatch": [
                {
                    "macro_namespace": "dbt",
                    "search_order": ["test", "package_macro_overrides", "dbt"],
                }
            ],
            "models": {
                "schema": "dbt_test"
            },
            
        }
        
    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    "local": "./package_macro_overrides",
                },
            ]
        }

    @use_profile('postgres')
    def test__postgres__custom_schema_from_macro(self):
        self.run_dbt(["deps"])
        super().test__postgres__custom_schema_from_macro


class TestCustomSchemaWithCustomMacroConfigs(TestCustomSchemaWithCustomMacro):

    @property
    def schema(self):
        return "custom_macro_cfg_024"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['custom-macros-configs'],
            'models': {
                'schema': 'dbt_test'
            },
        }

    @use_profile('postgres')
    def test__postgres__custom_schema_from_macro(self):
        self.run_sql_file("seed.sql")
        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema()
        v1_schema = "dbt_test_{}_macro".format(schema)
        v2_schema = "custom_{}_macro".format(schema)
        xf_schema = "test_{}_macro".format(schema)

        self.assertTablesEqual("seed", "view_1", schema, v1_schema)
        self.assertTablesEqual("seed", "view_2", schema, v2_schema)
        self.assertTablesEqual("agg", "view_3", schema, xf_schema)


class TestCustomSchemaWithCustomMacroFromModelName(DBTIntegrationTest):
    def setUp(self):
        super().setUp()

        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v1_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v2_schema())
        )

    @property
    def schema(self):
        return "custom_macro_024"

    @property
    def models(self):
        return "dot-models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'prod': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema(),
                    }
                },
                'target': 'prod'
            }
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['custom-macros-multischema'],
            'seed-paths': ['seed'],
            'seeds': {
                'quote_columns': False,
            },            
            'models': {
                'schema': 'dbt_test',
            }
        }

    def v1_schema(self):
        return f"first_schema"

    def v2_schema(self):
        return f"second_schema"

    @use_profile('postgres')
    def test__postgres__custom_schema_from_macro_model_name(self):
        self.run_sql_file("seed.sql")
        default_schema = self.unique_schema()

        results = self.run_dbt(['seed'])
        self.assertEqual(len(results), 2)
        self.assertTableDoesExist("my_seed", self.v1_schema())
        self.assertTableDoesExist("my_agg", self.v2_schema())

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed", "view_1", default_schema, self.v1_schema())
        self.assertTablesEqual("seed", "view_2", default_schema, self.v2_schema())
        self.assertTablesEqual("agg", "view_3", default_schema, default_schema)

