from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestSimpleCopy(DBTIntegrationTest):

    def setUp(self):
        pass

    @property
    def schema(self):
        return "simple_copy_001"

    @property
    def models(self):
        return "test/integration/001_simple_copy_test/models"

    def run__simple_copy(self, adapter_type, file_suffix=''):
        self.run__simple_copy_first_pass(adapter_type, file_suffix)
        self.run__simple_copy_update(adapter_type, file_suffix)

    def run__simple_copy_first_pass(self, adapter_type, file_suffix=''):
        self.use_default_project()
        self.use_profile(adapter_type)
        self.run_sql_file("test/integration/001_simple_copy_test/seed{}.sql"
                          .format(file_suffix))

        self.run_dbt()

        self.assertTablesEqual("seed", "view")
        self.assertTablesEqual("seed", "incremental")
        self.assertTablesEqual("seed", "materialized")

    def run__simple_copy_update(self, adapter_type, file_suffix=''):
        self.use_default_project()
        self.use_profile(adapter_type)

        self.run_sql_file("test/integration/001_simple_copy_test/update{}.sql"
                          .format(file_suffix))

        self.run_dbt()

        self.assertTablesEqual("seed", "view")
        self.assertTablesEqual("seed", "incremental")
        self.assertTablesEqual("seed", "materialized")

    @attr(type='postgres')
    def test__postgres__simple_copy(self):
        self.run__simple_copy('postgres')

    @attr(type='snowflake')
    def test__snowflake__simple_copy(self):
        self.run__simple_copy('snowflake')

    @attr(type='azure_dw')
    def test__azure_dw__simple_copy(self):
        self.run__simple_copy('azure_dw')

    @attr(type='sql_server')
    def test__sql_server__simple_copy(self):
        self.run__simple_copy_first_pass('sql_server', '_mssql')

    @attr(type='redshift')
    def test__redshift__simple_copy(self):
        self.run__simple_copy('redshift')

    @attr(type='postgres')
    def test__postgres__dbt_doesnt_run_empty_models(self):
        self.use_default_project()
        self.use_profile('postgres')
        self.run_sql_file("test/integration/001_simple_copy_test/seed.sql")

        self.run_dbt()

        models = self.get_models_in_schema()

        self.assertFalse('empty' in models.keys())
        self.assertFalse('disabled' in models.keys())
