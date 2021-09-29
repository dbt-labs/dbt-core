import json
import os
from pytest import mark

from tests.integration.base import DBTIntegrationTest, use_profile


class BaseTestSimpleCopy(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_copy"

    @staticmethod
    def dir(path):
        return path.lstrip('/')

    @property
    def models(self):
        return self.dir("models")

    @property
    def project_config(self):
        return self.seed_quote_cfg_with({
            'profile': '{{ "tes" ~ "t" }}'
        })

    def seed_quote_cfg_with(self, extra):
        cfg = {
            'config-version': 2,
            'seeds': {
                'quote_columns': False,
            }
        }
        cfg.update(extra)
        return cfg


class TestSimpleCopy(BaseTestSimpleCopy):

    @property
    def project_config(self):
        return self.seed_quote_cfg_with({"data-paths": [self.dir("seed-initial")]})

    @use_profile("bigquery")
    def test__bigquery__simple_copy(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")
        self.assertTablesEqual("seed", "materialized")
        self.assertTablesEqual("seed", "get_and_ref")

        self.use_default_project({"data-paths": [self.dir("seed-update")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")
        self.assertTablesEqual("seed", "materialized")
        self.assertTablesEqual("seed", "get_and_ref")


class TestIncrementalMergeColumns(BaseTestSimpleCopy):
    @property
    def models(self):
        return self.dir("models-merge-update")

    @property
    def project_config(self):
        return {
            "seeds": {
                "quote_columns": False
            }
        }

    def seed_and_run(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])

    @use_profile("bigquery")
    def test__bigquery__incremental_merge_columns(self):
        self.use_default_project({
            "data-paths": ["seeds-merge-cols-initial"]
        })
        self.seed_and_run()
        self.use_default_project({
            "data-paths": ["seeds-merge-cols-update"]
        })
        self.seed_and_run()
        self.assertTablesEqual("incremental_update_cols", "expected_result")
