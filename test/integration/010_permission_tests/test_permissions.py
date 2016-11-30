from test.integration.base import DBTIntegrationTest

class TestPermissions(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/010_permission_tests/seed.sql")

    def tearDown(self):
        DBTIntegrationTest.tearDown(self)

        self.run_sql_file("test/integration/010_permission_tests/tearDown.sql")

    @property
    def schema(self):
        return "permission_tests_010"

    @property
    def models(self):
        return "test/integration/010_permission_tests/models"

    def test_read_permissions(self):

        failed = False

        # run model as the noaccess user
        # this will fail with a RuntimeError, which should be caught by the dbt runner
        self.run_dbt(['run', '--target', 'noaccess'])

