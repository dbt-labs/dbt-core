from pytest import mark
from test.integration.base import DBTIntegrationTest, use_profile


class TestChangingRelationType(DBTIntegrationTest):

    @property
    def schema(self):
        return "changing_relation_type_035"

    @staticmethod
    def dir(path):
        return path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    def swap_types_and_test(self):
        # test that dbt is able to do intelligent things when changing
        # between materializations that create tables and views.

        results = self.run_dbt(['run', '--vars', 'materialized: view'])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: table'])
        self.assertEqual(results[0].node.config.materialized, 'table')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: view'])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: incremental'])
        self.assertEqual(results[0].node.config.materialized, 'incremental')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: view'])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)

    @use_profile("postgres")
    def test__postgres__switch_materialization(self):
        self.swap_types_and_test()
