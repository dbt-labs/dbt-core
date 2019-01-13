from test.integration.base import DBTIntegrationTest, use_profile
import os


class TestBootstrap(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_041"

    def unique_schema(self):
        return super(TestBootstrap, self).unique_schema()

    def tearDown(self):
        files = os.listdir(self.models())
        for f in files:
            if f.endswith(".yml"):
                os.remove(self.dir('models/'+f))

    @staticmethod
    def dir(path):
        return "test/integration/010_bootstrap_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    def check_bootstrap_completeness(self):
        self.run_dbt(["run"])
        results = self.run_dbt(["bootstrap", '--schemas', self.schema])

        self.assertTrue(os.path.isfile(self.path('models/model_a.yml')))
        self.assertTrue(os.path.isfile(self.path('models/model_b.yml')))

        import ipdb; ipdb.set_trace()


    def test_late_binding_view(self):
        pass
