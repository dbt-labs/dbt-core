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

    @use_profile("snowflake")
    def test__snowflake__simple_copy(self):
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "seeds": {
                'quote_columns': False,
            }
        })
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("seed-update")],
        })
        self.run_dbt(['test'])

    @use_profile("snowflake")
    def test__snowflake__simple_copy__quoting_off(self):
        self.use_default_project({
            "quoting": {"identifier": False},
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": False},
        })
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": False},
        })
        self.run_dbt(['test'])

    @use_profile("snowflake")
    def test__snowflake__seed__quoting_switch(self):
        self.use_default_project({
            "quoting": {"identifier": False},
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": True},
        })
        results = self.run_dbt(["seed"], expect_pass=False)

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("snowflake-seed-initial")],
        })
        self.run_dbt(['test'])


class TestSimpleCopyQuotingIdentifierOn(BaseTestSimpleCopy):
    @property
    def project_config(self):
        return self.seed_quote_cfg_with({
            'quoting': {
                'identifier': True,
            },
        })

    @use_profile("snowflake")
    def test__snowflake__simple_copy__quoting_on(self):
        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
        })
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

        # can't run the test as this one's identifiers will be the wrong case


class BaseLowercasedSchemaTest(BaseTestSimpleCopy):
    def unique_schema(self):
        # bypass the forced uppercasing that unique_schema() does on snowflake
        return super().unique_schema().lower()


class TestSnowflakeSimpleLowercasedSchemaCopy(BaseLowercasedSchemaTest):
    @use_profile('snowflake')
    def test__snowflake__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("snowflake-seed-initial")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({"data-paths": [self.dir("snowflake-seed-update")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({
            "test-paths": [self.dir("tests")],
            "data-paths": [self.dir("snowflake-seed-update")],
        })
        self.run_dbt(['test'])


class TestSnowflakeSimpleLowercasedSchemaQuoted(BaseLowercasedSchemaTest):
    @property
    def project_config(self):
        return self.seed_quote_cfg_with({
            'quoting': {'identifier': False, 'schema': True}
        })

    @use_profile("snowflake")
    def test__snowflake__seed__quoting_switch_schema_lower(self):
        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        # this is intentional - should not error!
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": False, "schema": False},
        })
        results = self.run_dbt(["seed"], expect_pass=False)


class TestSnowflakeSimpleUppercasedSchemaQuoted(BaseTestSimpleCopy):
    @property
    def project_config(self):
        return self.seed_quote_cfg_with({
            'quoting': {'identifier': False, 'schema': True}
        })

    @use_profile("snowflake")
    def test__snowflake__seed__quoting_switch_schema_upper(self):
        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-initial")],
        })

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        # this is intentional - should not error!
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-update")],
            "quoting": {"identifier": False, "schema": False},
        })
        results = self.run_dbt(["seed"])


class TestSnowflakeIncrementalOverwrite(BaseTestSimpleCopy):
    @property
    def models(self):
        return self.dir("models-snowflake")

    @use_profile("snowflake")
    def test__snowflake__incremental_overwrite(self):
        self.use_default_project({
            "data-paths": [self.dir("snowflake-seed-initial")],
        })
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  1)

        results = self.run_dbt(["run"], expect_pass=False)
        self.assertEqual(len(results),  1)

        # Setting the incremental_strategy should make this succeed
        self.use_default_project({
            "models": {
                "incremental_strategy": "delete+insert"
            },
            "data-paths": [self.dir("snowflake-seed-update")],
        })

        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  1)


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

    @use_profile("snowflake")
    def test__snowflake__incremental_merge_columns(self):
        self.use_default_project({
            "data-paths": ["seeds-merge-cols-initial"],
            "seeds": {
                "quote_columns": False
            }
        })
        self.seed_and_run()
        self.use_default_project({
            "data-paths": ["seeds-merge-cols-update"],
            "seeds": {
                "quote_columns": False
            }
        })
        self.seed_and_run()
        self.assertTablesEqual("incremental_update_cols", "expected_result")
