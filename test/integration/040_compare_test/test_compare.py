from test.integration.base import DBTIntegrationTest, use_profile
import os


class TestCompare(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_040"

    def unique_schema(self):
        return super(TestCompare, self).unique_schema()

    @staticmethod
    def dir(path):
        return "test/integration/040_compare_test/" + path.lstrip("/")

    def tearDown(self):
        os.remove(self.dir("models/test_view.sql"))

    @property
    def models(self):
        return self.dir("models")

    def create_test_model(self, materialization):
        model = """
            {{{{ config(materialized='{mat}') }}}}
            SELECT 1 AS colname
            """.format(
            mat=materialization
        )

        with open(self.dir("models/test_view.sql"), "w") as f:
            f.write(model)

    def get_created_models(self):
        relations = self.adapter.list_relations_without_caching(
                self.unique_schema()
                )
        created_models = [rel.table.lower() for rel in relations]
        return created_models

    def compare_switch_to_ephemeral(self):

        # Run dbt with test_view as a view
        self.create_test_model("view")
        self.run_dbt(["run"])

        # Assert the view exists in the database
        created_models = self.get_created_models()

        self.assertTrue("test_view" in created_models)
        self.assertTrue("downstream" in created_models)

        # Assert dbt compare passes
        results = self.run_dbt(["compare"])

        self.assertTrue(len(results) == 0)

        # Run dbt with test_view as ephemeral
        self.create_test_model("ephemeral")
        self.run_dbt(["run"])
        # Assert the view still exists in the database
        created_models = self.get_created_models()
        self.assertTrue("test_view" in created_models)
        self.assertTrue("downstream" in created_models)
        # Assert dbt compare fails
        results = self.run_dbt(["compare"], expect_pass=False)
        self.assertTrue(len(results) == 1)

    @use_profile("postgres")
    def test__postgres__compare(self):
        self.compare_switch_to_ephemeral()

    @use_profile("redshift")
    def test__redshift__compare(self):
        self.compare_switch_to_ephemeral()

    @use_profile("snowflake")
    def test__snowflake__compare(self):
        self.compare_switch_to_ephemeral()

    @use_profile("bigquery")
    def test__bigquery__compare(self):
        self.compare_switch_to_ephemeral()
