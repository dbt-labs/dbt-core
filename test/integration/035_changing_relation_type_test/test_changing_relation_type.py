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

    @use_profile("snowflake")
    def test__snowflake__switch_materialization(self):
        self.swap_types_and_test()

    @use_profile("redshift")
    def test__redshift__switch_materialization(self):
        self.swap_types_and_test()

    @use_profile("bigquery")
    def test__bigquery__switch_materialization(self):
        # BQ has a weird check that prevents the dropping of tables in the view materialization
        # if --full-refresh is not provided. This is to prevent the clobbering of a date-sharded
        # table with a view if a model config is accidently changed. We should probably remove that check
        # and then remove these bq-specific tests

        results = self.run_dbt(['run', '--vars', 'materialized: view'])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: table'])
        self.assertEqual(results[0].node.config.materialized, 'table')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: view', "--full-refresh"])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: incremental'])
        self.assertEqual(results[0].node.config.materialized, 'incremental')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: view', "--full-refresh"])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)

    @use_profile('presto')
    def test__presto__switch_materialization(self):
        # presto can't do incremental materializations so there's less to this

        results = self.run_dbt(['run', '--vars', 'materialized: view'])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: table'])
        self.assertEqual(results[0].node.config.materialized, 'table')
        self.assertEqual(len(results),  1)

        results = self.run_dbt(['run', '--vars', 'materialized: view'])
        self.assertEqual(results[0].node.config.materialized, 'view')
        self.assertEqual(len(results),  1)
