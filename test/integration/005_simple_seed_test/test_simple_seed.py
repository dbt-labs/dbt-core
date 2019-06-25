import os

from test.integration.base import DBTIntegrationTest, use_profile

from dbt.exceptions import CompilationException


class TestSimpleSeed(DBTIntegrationTest):

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
            "data-paths": ['data']
        }

    @use_profile('postgres')
    def test_simple_seed(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected")

        # this should truncate the seed_actual table, then re-insert.
        # also, '--show' should not crash dbt!
        results = self.run_dbt(["seed", '--show'])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected")


    @use_profile('postgres')
    def test_simple_seed_with_drop(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected")

        # this should drop the seed table, then re-create
        results = self.run_dbt(["seed", "--full-refresh"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected")


class TestSimpleSeedCustomSchema(DBTIntegrationTest):

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
            "data-paths": ['data'],
            "seeds": {
                "schema": "custom_schema"
            }
        }

    @use_profile('postgres')
    def test_simple_seed_with_schema(self):
        schema_name = "{}_{}".format(self.unique_schema(), 'custom_schema')

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected", table_a_schema=schema_name)

        # this should truncate the seed_actual table, then re-insert
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_actual","seed_expected", table_a_schema=schema_name)


    @use_profile('postgres')
    def test_simple_seed_with_drop_and_schema(self):
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
            "data-paths": ['data-config'],
            "seeds": {
                "test": {
                    "seed_enabled": {
                        "enabled": True
                    },
                    "seed_disabled": {
                        "enabled": False
                    }
                }
            }
        }

    @use_profile('postgres')
    def test_simple_seed_with_disabled(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTableDoesExist('seed_enabled')
        self.assertTableDoesNotExist('seed_disabled')


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
            "data-paths": ['data-bad']
        }

    @use_profile('postgres')
    def test_postgres_dbt_run_skips_seeds(self):
        # run does not try to parse the seed files
        self.assertEqual(len(self.run_dbt(['run'])), 1)

        # make sure 'dbt seed' fails, otherwise our test is invalid!
        with self.assertRaises(CompilationException):
            self.run_dbt(['seed'])


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
            "data-paths": ['data-bom']
        }

    @use_profile('postgres')
    def test_simple_seed(self):
        # first make sure nobody "fixed" the file by accident
        seed_path = os.path.join(self.config.data_paths[0], 'seed_bom.csv')
        # 'data-bom/seed_bom.csv'
        with open(seed_path, encoding='utf-8') as fp:
            self.assertEqual(fp.read(1), u'\ufeff')
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assertTablesEqual("seed_bom", "seed_expected")
