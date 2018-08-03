from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestPermissions(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/010_permission_tests/seed.sql")

    def tearDown(self):
        self.run_sql_file("test/integration/010_permission_tests/tearDown.sql")

        DBTIntegrationTest.tearDown(self)

    @property
    def schema(self):
        return "permission_tests_010"

    @property
    def models(self):
        return "test/integration/010_permission_tests/models"

    @attr(type='postgres')
    def test_create_schema_permissions(self):
        # the noaccess user does not have permissions to create a schema -- this should fail

        failed = False
        self.run_sql('drop schema if exists "{}" cascade'.format(self.unique_schema()))
        try:
            self.run_dbt(['run', '--target', 'noaccess'], expect_pass=False)
        except RuntimeError:
            failed = True

        self.assertTrue(failed)

        self.run_sql_file("test/integration/010_permission_tests/seed.sql")

        # now it should work!
        self.run_sql('grant create on database dbt to noaccess'.format(self.unique_schema()))
        self.run_sql('grant usage, create on schema "{}" to noaccess'.format(self.unique_schema()))
        self.run_sql('grant select on all tables in schema "{}" to noaccess'.format(self.unique_schema()))

        results = self.run_dbt(['run', '--target', 'noaccess'])
        self.assertEqual(len(results), 1)
