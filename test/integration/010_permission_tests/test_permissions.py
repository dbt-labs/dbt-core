from test.integration.base import DBTIntegrationTest, use_profile

class TestPermissions(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/010_permission_tests/seed.sql")

    @property
    def schema(self):
        return "permission_tests_010"

    @property
    def models(self):
        return "test/integration/010_permission_tests/models"

    @use_profile('postgres')
    def test_no_create_schema_permissions(self):
        # the noaccess user does not have permissions to create a schema -- this should fail
        failed = False
        self.run_sql('drop schema if exists "{}" cascade'.format(self.unique_schema()))
        try:
            self.run_dbt(['run', '--target', 'noaccess'], expect_pass=False)
        except RuntimeError:
            failed = True

        self.assertTrue(failed)

    @use_profile('postgres')
    def test_create_schema_permissions(self):
        # now it should work!
        self.run_sql('grant create on database {} to noaccess'.format(self.default_database))
        self.run_sql('grant usage, create on schema "{}" to noaccess'.format(self.unique_schema()))
        self.run_sql('grant select on all tables in schema "{}" to noaccess'.format(self.unique_schema()))

        results = self.run_dbt(['run', '--target', 'noaccess'])
        self.assertEqual(len(results), 1)
