import os
import csv
from test.integration.base import DBTIntegrationTest, use_profile


class TestSimpleSeed(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "models-downstream-seed"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data'],
            'seeds': {
                'quote_columns': False,
            }
        }

    def use_full_refresh_project(self, full_refresh: bool):
        overrides = {
            'seeds': {
                'quote_columns': False,
                'full_refresh': full_refresh,
            }
        }
        self.use_default_project(overrides)

    def _seed_and_run(self):
        assert len(self.run_dbt(['seed'])) == 1
        self.assertTablesEqual('seed_actual', 'seed_expected')

        assert len(self.run_dbt(['run'])) == 1
        self.assertTablesEqual('model', 'seed_expected')

    def _after_seed_model_state(self, cmd, exists: bool):
        assert len(self.run_dbt(cmd)) == 1
        self.assertTablesEqual('seed_actual', 'seed_expected')
        if exists:
            self.assertTableDoesExist('model')
        else:
            self.assertTableDoesNotExist('model')

    @use_profile('postgres')
    def test_postgres_simple_seed(self):
        self._seed_and_run()

        # this should truncate the seed_actual table, then re-insert.
        self._after_seed_model_state(['seed'], exists=True)

    @use_profile('postgres')
    def test_postgres_simple_seed_full_refresh_flag(self):
        self._seed_and_run()

        # this should drop the seed_actual table, then re-create it, so the
        # model won't exist.
        self._after_seed_model_state(['seed', '--full-refresh'], exists=False)

    @use_profile('postgres')
    def test_postgres_simple_seed_full_refresh_config(self):
        self._seed_and_run()

        # set the full_refresh config to False
        self.use_full_refresh_project(False)

        self._after_seed_model_state(['seed'], exists=True)
        # make sure we ignore the full-refresh flag (the config is higher
        # priority than the flag)
        self._after_seed_model_state(['seed', '--full-refresh'], exists=True)

        # this should drop the seed_actual table, then re-create it, so the
        # model won't exist.
        self.use_full_refresh_project(True)
        self._after_seed_model_state(['seed'], exists=False)


class TestSimpleSeedCustomSchema(DBTIntegrationTest):

    def setUp(self):
        super().setUp()
        self.run_sql_file("seed.sql")
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.custom_schema_name())
        )

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data'],
            'seeds': {
                "schema": "custom_schema",
                'quote_columns': False,
            },
        }

    def custom_schema_name(self):
        return "{}_{}".format(self.unique_schema(), 'custom_schema')

    @use_profile('postgres')
    def test_postgres_simple_seed_with_schema(self):
        schema_name = self.custom_schema_name()

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected", table_a_schema=schema_name)

        # this should truncate the seed_actual table, then re-insert
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected", table_a_schema=schema_name)

    @use_profile('postgres')
    def test_postgres_simple_seed_with_drop_and_schema(self):
        schema_name = "{}_{}".format(self.unique_schema(), 'custom_schema')

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected", table_a_schema=schema_name)

        # this should drop the seed table, then re-create
        results = self.run_dbt(["seed", "--full-refresh"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected", table_a_schema=schema_name)


class TestSimpleSeedDisabled(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data-config'],
            'seeds': {
                "test": {
                    "seed_enabled": {
                        "enabled": True
                    },
                    "seed_disabled": {
                        "enabled": False
                    }
                },
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_simple_seed_with_disabled(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  2)
        self.assertTableDoesExist('seed_enabled')
        self.assertTableDoesNotExist('seed_disabled')

    @use_profile('postgres')
    def test_postgres_simple_seed_selection(self):
        results = self.run_dbt(['seed', '--select', 'seed_enabled'])
        self.assertEqual(len(results),  1)
        self.assertTableDoesExist('seed_enabled')
        self.assertTableDoesNotExist('seed_disabled')
        self.assertTableDoesNotExist('seed_tricky')

    @use_profile('postgres')
    def test_postgres_simple_seed_exclude(self):
        results = self.run_dbt(['seed', '--exclude', 'seed_enabled'])
        self.assertEqual(len(results),  1)
        self.assertTableDoesNotExist('seed_enabled')
        self.assertTableDoesNotExist('seed_disabled')
        self.assertTableDoesExist('seed_tricky')


class TestSeedParsing(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "models-exist"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data-bad'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_dbt_run_skips_seeds(self):
        # run does not try to parse the seed files
        self.assertEqual(len(self.run_dbt(['run'])), 1)

        # make sure 'dbt seed' fails, otherwise our test is invalid!
        self.run_dbt(['seed'], expect_pass=False)


class TestSimpleSeedWithBOM(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data-bom'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_simple_seed(self):
        # first make sure nobody "fixed" the file by accident
        seed_path = os.path.join(self.config.data_paths[0], 'seed_bom.csv')
        # 'data-bom/seed_bom.csv'
        with open(seed_path, encoding='utf-8') as fp:
            self.assertEqual(fp.read(1), u'\ufeff')
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_bom", "seed_expected")


class TestSimpleSeedWithUnicode(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data-unicode'],
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile('postgres')
    def test_postgres_simple_seed(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)


class TestSimpleSeedWithDots(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "data-paths": ['data-dottedseed'],
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile('postgres')
    def test_postgres_simple_seed(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        
class TestSimpleBigSeedBatched(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_seed_005"

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


    @use_profile('postgres')
    def test_postgres_big_batched_seed(self):
        self.test_big_batched_seed()
    