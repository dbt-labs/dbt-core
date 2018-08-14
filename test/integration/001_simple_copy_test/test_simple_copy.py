from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, use_profile


class TestSimpleCopy(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_copy_001"

    @staticmethod
    def dir(path):
        return "test/integration/001_simple_copy_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    @use_profile("postgres")
    def test__postgres__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

    @use_profile("postgres")
    def test__postgres__dbt_doesnt_run_empty_models(self):
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        models = self.get_models_in_schema()

        self.assertFalse("empty" in models.keys())
        self.assertFalse("disabled" in models.keys())

    @use_profile("snowflake")
    def test__snowflake__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

    @use_profile("snowflake")
    def test__snowflake__simple_copy__quoting_on(self):
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "quoting": {"identifier": True},
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

        self.use_default_project({
            "data-paths": [self.dir("seed-update")],
            "quoting": {"identifier": True},
        })
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

    @use_profile("snowflake")
    def test__snowflake__simple_copy__quoting_off(self):
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "quoting": {"identifier": False},
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED"])

        self.use_default_project({
            "data-paths": [self.dir("seed-update")],
            "quoting": {"identifier": False},
        })
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED"])

    @use_profile("snowflake")
    def test__snowflake__seed__quoting_switch(self):
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "quoting": {"identifier": False},
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        self.use_default_project({
            "data-paths": [self.dir("seed-update")],
            "quoting": {"identifier": True},
        })
        results = self.run_dbt(["seed"], expect_pass=False)

    @use_profile("bigquery")
    def test__bigquery__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

        self.use_default_project({"data-paths": [self.dir("seed-update")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  6)

        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")


class TestSimpleCopyLowercasedSchema(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_copy_001"

    @staticmethod
    def dir(path):
        return "test/integration/001_simple_copy_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    def unique_schema(self):
        # bypass the forced uppercasing that unique_schema() does on snowflake
        schema = super(TestSimpleCopyLowercasedSchema, self).unique_schema()
        return schema.lower()

    @use_profile('snowflake')
    def test__snowflake__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized"])

    @use_profile("snowflake")
    def test__snowflake__seed__quoting_switch_schema(self):
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "quoting": {"identifier": False, "schema": True},
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        self.use_default_project({
            "data-paths": [self.dir("seed-update")],
            "quoting": {"identifier": False, "schema": False},
        })
        results = self.run_dbt(["seed"], expect_pass=False)
