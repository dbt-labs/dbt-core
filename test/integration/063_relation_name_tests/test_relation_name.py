from test.integration.base import DBTIntegrationTest, use_profile
import dbt.exceptions

class TestAdapterDDL(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_dbt(['seed'])

    @property
    def schema(self):
        return "adapter_ddl_063"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_long_name_fails(self):
        self.run_dbt(['run'],expect_pass=False)

    @use_profile('redshift')
    def test_redshift_long_name_succeeds(self):
        self.run_dbt(['run'],expect_pass=True)

