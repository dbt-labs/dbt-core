from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import yaml


class TestBigqueryAdapterFunctions(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "adapter-models"

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @use_profile('bigquery')
    def test__bigquery_adapter_functions(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        test_results = self.run_dbt(['test'])

        self.assertTrue(len(test_results) > 0)
        for result in test_results:
            self.assertEqual(result.status, 'pass')
            self.assertFalse(result.skipped)
            self.assertEqual(int(result.message), 0)


class TestBigqueryAdapterMacros(DBTIntegrationTest):
    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "models"

    def _create_schema_named(self, database, schema):
        # do not create the initial schema. We'll do this ourselves!
        pass

    @use_profile('bigquery')
    def test__bigquery_run_create_drop_schema(self):
        schema_args = yaml.safe_dump({
            'db_name': self.default_database,
            'schema_name': self.unique_schema(),
        })
        self.run_dbt(
            ['run-operation', 'my_create_schema', '--args', schema_args])
        relation_args = yaml.safe_dump({
            'db_name': self.default_database,
            'schema_name': self.unique_schema(),
            'table_name': 'some_table',
        })
        self.run_dbt(['run-operation', 'my_create_table_as',
                      '--args', relation_args])
        # exercise list_relations_without_caching and get_columns_in_relation
        self.run_dbt(
            ['run-operation', 'ensure_one_relation_in', '--args', schema_args])
        # now to drop the schema
        schema_relation = self.adapter.Relation.create(
            database=self.default_database, schema=self.unique_schema()).without_identifier()
        with self.adapter.connection_named('test'):
            results = self.adapter.list_relations_without_caching(
                schema_relation)
        assert len(results) == 1

        self.run_dbt(
            ['run-operation', 'my_drop_schema', '--args', schema_args])
        with self.adapter.connection_named('test'):
            results = self.adapter.list_relations_without_caching(
                schema_relation)
        assert len(results) == 0
