from test.integration.base import DBTIntegrationTest, use_profile


class BaseTestSimpleCopy(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_copy_001"

    @staticmethod
    def dir(path):
        return path.lstrip('/')

    @property
    def models(self):
        return self.dir("models")


class TestSimpleCopy(BaseTestSimpleCopy):

    @property
    def project_config(self):
        return {"data-paths": [self.dir("seed-initial")]}

    @use_profile("postgres")
    def test__postgres__simple_copy(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

    @use_profile('postgres')
    def test__postgres__simple_copy_with_materialized_views(self):
        self.run_sql('''
            create table {schema}.unrelated_table (id int)
        '''.format(schema=self.unique_schema())
        )
        self.run_sql('''
            create materialized view {schema}.unrelated_materialized_view as (
                select * from {schema}.unrelated_table
            )
        '''.format(schema=self.unique_schema()))
        self.run_sql('''
            create view {schema}.unrelated_view as (
                select * from {schema}.unrelated_materialized_view
            )
        '''.format(schema=self.unique_schema()))

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

    @use_profile("postgres")
    def test__postgres__dbt_doesnt_run_empty_models(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        models = self.get_models_in_schema()

        self.assertFalse("empty" in models.keys())
        self.assertFalse("disabled" in models.keys())

    @use_profile("presto")
    def test__presto__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results),  7)
        for result in results:
            if 'incremental' in result.node.name:
                self.assertIn('not implemented for presto', result.error)

        self.assertManyTablesEqual(["seed", "view_model", "materialized"])

    @use_profile("snowflake")
    def test__snowflake__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("snowflake-seed-initial")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({"data-paths": [self.dir("snowflake-seed-update")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("snowflake-seed-update")],
        })
        self.run_dbt(['test'])

    @use_profile("snowflake")
    def test__snowflake__simple_copy__quoting_off(self):
        self.use_default_project({
            "quoting": {"identifier": False},
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": False},
        })
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": False},
        })
        self.run_dbt(['test'])

    @use_profile("snowflake")
    def test__snowflake__seed__quoting_switch(self):
        self.use_default_project({
            "quoting": {"identifier": False},
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": True},
        })
        results = self.run_dbt(["seed"], expect_pass=False)

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("snowflake-seed-initial")],
        })
        self.run_dbt(['test'])

    @use_profile("bigquery")
    def test__bigquery__simple_copy(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")
        self.assertTablesEqual("seed", "materialized")
        self.assertTablesEqual("seed", "get_and_ref")

        self.use_default_project({"data-paths": [self.dir("seed-update")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")
        self.assertTablesEqual("seed", "materialized")
        self.assertTablesEqual("seed", "get_and_ref")


class TestSimpleCopyQuotingIdentifierOn(BaseTestSimpleCopy):
    @property
    def project_config(self):
        return {
            'quoting': {
                'identifier': True,
            },
        }

    @use_profile("snowflake")
    def test__snowflake__simple_copy__quoting_on(self):
        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
        })
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

        # can't run the test as this one's identifiers will be the wrong case


class BaseLowercasedSchemaTest(BaseTestSimpleCopy):
    def unique_schema(self):
        # bypass the forced uppercasing that unique_schema() does on snowflake
        return super().unique_schema().lower()


class TestSnowflakeSimpleLowercasedSchemaCopy(BaseLowercasedSchemaTest):
    @use_profile('snowflake')
    def test__snowflake__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("snowflake-seed-initial")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({"data-paths": [self.dir("snowflake-seed-update")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("snowflake-seed-update")],
        })
        self.run_dbt(['test'])


class TestSnowflakeSimpleLowercasedSchemaQuoted(BaseLowercasedSchemaTest):
    @property
    def project_config(self):
        return {
            'quoting': {'identifier': False, 'schema': True}
        }

    @use_profile("snowflake")
    def test__snowflake__seed__quoting_switch_schema(self):
        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": False, "schema": False},
        })
        results = self.run_dbt(["seed"], expect_pass=False)


class TestSnowflakeIncrementalOverwrite(BaseTestSimpleCopy):
    @property
    def models(self):
        return self.dir("models-snowflake")

    @use_profile("snowflake")
    def test__snowflake__incremental_overwrite(self):
        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-initial")],
        })
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  1)

        results = self.run_dbt(["run"], expect_pass=False)
        self.assertEqual(len(results),  1)

        # Setting the incremental_strategy should make this succeed
        self.use_default_project({
            "models": {"incremental_strategy": "delete+insert"},
            "data-paths": [self.dir("snowflake-seed-update")],
        })

        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  1)


class TestShouting(BaseTestSimpleCopy):
    @property
    def models(self):
        return self.dir('shouting_models')

    @property
    def project_config(self):
        return {"data-paths": [self.dir("seed-initial")]}

    @use_profile("postgres")
    def test__postgres__simple_copy_loud(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])
