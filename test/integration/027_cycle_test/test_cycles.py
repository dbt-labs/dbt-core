from freezegun import freeze_time
from test.integration.base import DBTIntegrationTest, use_profile


class TestSimpleCycle(DBTIntegrationTest):

    @property
    def schema(self):
        return "cycles_simple_025"

    @property
    def models(self):
        return "test/integration/027_cycle_test/simple_cycle_models"

    @property
    @use_profile('postgres')
    def test_simple_cycle(self):
        message = "Found a cycle.*"
        with self.assertRaisesRegexp(Exception, message):
            self.run_dbt(["run"])

class TestComplexCycle(DBTIntegrationTest):

    @property
    def schema(self):
        return "cycles_complex_025"

    @property
    def models(self):
        return "test/integration/027_cycle_test/complex_cycle_models"

    @property
    @use_profile('postgres')
    def test_simple_cycle(self):
        message = "Found a cycle.*"
        with self.assertRaisesRegexp(Exception, message):
            self.run_dbt(["run"])
