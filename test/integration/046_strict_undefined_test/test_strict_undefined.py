from test.integration.base import DBTIntegrationTest, use_profile


class TestStrictUndefined(DBTIntegrationTest):

    @property
    def schema(self):
        return 'strict_undefined_046'

    @property
    def models(self):
        return 'models'

    @use_profile('postgres')
    def test_postgres_strict_undefined(self):
        self.run_dbt(['run'], strict=True, expect_pass=False)

    @use_profile('postgres')
    def test_postgres_nonstrict_undefined(self):
        self.run_dbt(['run'], strict=False, expect_pass=True)
