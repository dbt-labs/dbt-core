from test.integration.base import DBTIntegrationTest, use_profile
from dbt.adapters.factory import FACTORY

class TestBaseCaching(DBTIntegrationTest):
    @property
    def schema(self):
        return "caching_038"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'quoting': {
                'identifier': False,
                'schema': False,
            }
        }

    def run_and_get_adapter(self):
        # we want to inspect the adapter that dbt used for the run, which is
        # not self.adapter. You can't do this until after you've run dbt once.
        self.run_dbt(['run'])
        return FACTORY.adapters[self.adapter_type]

    def cache_run(self):
        adapter = self.run_and_get_adapter()
        self.assertEqual(len(adapter.cache.relations), 1)
        relation = next(iter(adapter.cache.relations.values()))
        self.assertEqual(relation.inner.schema, self.unique_schema())
        self.assertEqual(relation.schema, self.unique_schema().lower())

        self.run_dbt(['run'])
        self.assertEqual(len(adapter.cache.relations), 1)
        second_relation = next(iter(adapter.cache.relations.values()))
        self.assertEqual(relation, second_relation)

class TestCachingLowercaseModel(TestBaseCaching):
    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_cache(self):
        self.cache_run()

class TestCachingUppercaseModel(TestBaseCaching):
    @property
    def models(self):
        return "shouting_models"

    @use_profile('postgres')
    def test_postgres_cache(self):
        self.cache_run()
