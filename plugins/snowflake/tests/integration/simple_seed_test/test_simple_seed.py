import os
import csv
from tests.integration.base import DBTIntegrationTest, use_profile

class TestSimpleBigSeedBatched(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_seed"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data-big'],
            'seeds': {
                'quote_columns': False,
            }
        }

    def test_big_batched_seed(self):
        with open('data-big/my_seed.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['id'])
            for i in range(0, 20000):
                writer.writerow([i])
            
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
    
    @use_profile('snowflake')
    def test_snowflake_big_batched_seed(self):
        self.test_big_batched_seed()
    