import json
import os
from pytest import mark

from test.integration.base import DBTIntegrationTest, use_profile


class BaseTestSimpleCopy(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_copy_001"

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

    @use_profile("postgres")
    def test__postgres__simple_copy(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt()
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

    @use_profile('postgres')
    def test__postgres__simple_copy_with_materialized_views(self):
        self.run_sql('''
            create table {schema}.unrelated_table (id int)
        '''.format(schema=self.unique_schema())
        )
        self.run_sql('''
            create materialized view {schema}.unrelated_materialized_view as (
                select * from {schema}.unrelated_table
            )
        '''.format(schema=self.unique_schema()))
        self.run_sql('''
            create view {schema}.unrelated_view as (
                select * from {schema}.unrelated_materialized_view
            )
        '''.format(schema=self.unique_schema()))
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

    @use_profile("postgres")
    def test__postgres__dbt_doesnt_run_empty_models(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        models = self.get_models_in_schema()

        self.assertFalse("empty" in models.keys())
        self.assertFalse("disabled" in models.keys())

    @use_profile("presto")
    def test__presto__simple_copy(self):
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results),  7)
        for result in results:
            if 'incremental' in result.node.name:
                self.assertIn('not implemented for presto', result.message)

        self.assertManyTablesEqual(["seed", "view_model", "materialized"])


class TestShouting(BaseTestSimpleCopy):
    @property
    def models(self):
        return self.dir('models-shouting')

    @property
    def project_config(self):
        return self.seed_quote_cfg_with({"data-paths": [self.dir("seed-initial")]})

    @use_profile("postgres")
    def test__postgres__simple_copy_loud(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])


# I give up on getting this working for Windows.
@mark.skipif(os.name == 'nt', reason='mixed-case postgres database tests are not supported on Windows')
class TestMixedCaseDatabase(BaseTestSimpleCopy):
    @property
    def models(self):
        return self.dir('models-trivial')

    def postgres_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbtMixedCase',
                        'schema': self.unique_schema()
                    },
                },
                'target': 'default2'
            }
        }

    @property
    def project_config(self):
        return {'config-version': 2}

    @use_profile('postgres')
    def test_postgres_run_mixed_case(self):
        self.run_dbt()
        self.run_dbt()


class TestQuotedDatabase(BaseTestSimpleCopy):

    @property
    def project_config(self):
        return self.seed_quote_cfg_with({
            'quoting': {
                'database': True,
            },
            "data-paths": [self.dir("seed-initial")],
        })

    def seed_get_json(self, expect_pass=True):
        results, output = self.run_dbt_and_capture(
            ['--debug', '--log-format=json', '--single-threaded', 'seed'],
            expect_pass=expect_pass
        )

        logs = []
        for line in output.split('\n'):
            try:
                log = json.loads(line)
            except ValueError:
                continue

            if log['extra'].get('run_state') != 'internal':
                continue
            logs.append(log)

        self.assertGreater(len(logs), 0)
        return logs

    @use_profile('postgres')
    def test_postgres_no_create_schemas(self):
        logs = self.seed_get_json()
        for log in logs:
            msg = log['message']
            self.assertFalse(
                'create schema if not exists' in msg,
                f'did not expect schema creation: {msg}'
            )
