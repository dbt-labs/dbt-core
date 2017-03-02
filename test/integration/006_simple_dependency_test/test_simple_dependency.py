from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestSimpleDependency(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/006_simple_dependency_test/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "test/integration/006_simple_dependency_test/models"

    @property
    def project_config(self):
        return {
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    @attr(type='postgres')
    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

        self.assertTablesEqual("seed_summary","view_summary")

        self.run_sql_file("test/integration/006_simple_dependency_test/update.sql")

        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

    @attr(type='postgres')
    def test_simple_dependency_with_models(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run", '--models', 'view+'])

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed_summary","view_summary")

        created_models = self.get_models_in_schema()

        self.assertFalse('table' in created_models)
        self.assertFalse('incremental' in created_models)

        self.assertEqual(created_models['view'], 'view')
        self.assertEqual(created_models['view_summary'], 'view')

class TestSimpleDependencyBranch(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/006_simple_dependency_test/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "test/integration/006_simple_dependency_test/models"

    @property
    def project_config(self):
        return {
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project@master'
            ]
        }

    def deps_run_assert_equality(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

        created_models = self.get_models_in_schema()

        self.assertEqual(created_models['table'], 'table')
        self.assertEqual(created_models['view'], 'view')
        self.assertEqual(created_models['view_summary'], 'view')
        self.assertEqual(created_models['incremental'], 'table')

    @attr(type='postgres')
    def test_simple_dependency(self):
        self.deps_run_assert_equality()

        self.assertTablesEqual("seed_summary","view_summary")

        self.run_sql_file("test/integration/006_simple_dependency_test/update.sql")

        self.deps_run_assert_equality()

    @attr(type='postgres')
    def test_empty_models_not_compiled_in_dependencies(self):
        self.deps_run_assert_equality()

        models = self.get_models_in_schema()

        self.assertFalse('empty' in models.keys())
