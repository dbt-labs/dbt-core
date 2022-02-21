import os
import tempfile
from test.integration.base import DBTIntegrationTest, use_profile
from dbt.exceptions import CompilationException, DependencyException
from dbt import deprecations


class TestSimpleDependency(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': '1.1',
                }
            ]
        }

    def run_deps(self):
        return self.run_dbt(["deps"])

    def run_clean(self):
        return self.run_dbt(['clean'])

    @use_profile('postgres')
    def test_postgres_simple_dependency(self):
        self.run_deps()
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  4)

        self.assertTablesEqual("seed", "table_model")
        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")

        self.assertTablesEqual("seed_summary", "view_summary")

        self.run_sql_file("update.sql")

        self.run_deps()
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  4)

        self.assertTablesEqual("seed", "table_model")
        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")

        assert os.path.exists('target')
        self.run_clean()
        assert not os.path.exists('target')

    @use_profile('postgres')
    def test_postgres_simple_dependency_with_models(self):
        self.run_deps()
        results = self.run_dbt(["run", '--models', 'view_model+'])
        self.assertEqual(len(results),  2)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed_summary", "view_summary")

        created_models = self.get_models_in_schema()

        self.assertFalse('table_model' in created_models)
        self.assertFalse('incremental' in created_models)

        self.assertEqual(created_models['view_model'], 'view')
        self.assertEqual(created_models['view_summary'], 'view')

        assert os.path.exists('target')
        self.run_clean()
        assert not os.path.exists('target')


class TestSimpleDependencyUnpinned(DBTIntegrationTest):
    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'warn-unpinned': True,
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_simple_dependency(self):
        self.run_dbt(["deps"])


class TestSimpleDependencyWithDuplicates(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        # dbt should convert these into a single dependency internally
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'dbt/1.0.0',
                },
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project.git',
                    'revision': 'dbt/1.0.0',
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_simple_dependency_deps(self):
        self.run_dbt(["deps"])


class TestRekeyedDependencyWithSubduplicates(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        # this revision of dbt-integration-project requires dbt-utils.git@0.5.0, which the
        # package config handling should detect
        return {
            'packages': [
                {

                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'config-1.0.0-deps'
                },
                {
                    'git': 'https://github.com/dbt-labs/dbt-utils',
                    'revision': '0.5.0',
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_simple_dependency_deps(self):
        self.run_dbt(["deps"])
        self.assertEqual(len(os.listdir('dbt_packages')), 2)


class TestSimpleDependencyBranch(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'dbt/1.0.0',
                },
            ]
        }

    def deps_run_assert_equality(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  4)

        self.assertTablesEqual("seed", "table_model")
        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")

        created_models = self.get_models_in_schema()

        self.assertEqual(created_models['table_model'], 'table')
        self.assertEqual(created_models['view_model'], 'view')
        self.assertEqual(created_models['view_summary'], 'view')
        self.assertEqual(created_models['incremental'], 'table')

    @use_profile('postgres')
    def test_postgres_simple_dependency(self):
        self.deps_run_assert_equality()

        self.assertTablesEqual("seed_summary", "view_summary")

        self.run_sql_file("update.sql")

        self.deps_run_assert_equality()

    @use_profile('postgres')
    def test_postgres_empty_models_not_compiled_in_dependencies(self):
        self.deps_run_assert_equality()

        models = self.get_models_in_schema()

        self.assertFalse('empty' in models.keys())


class TestSimpleDependencyNoProfile(TestSimpleDependency):
    def run_deps(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self.run_dbt(["deps", "--profiles-dir", tmpdir])
        return result

    def run_clean(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self.run_dbt(["clean", "--profiles-dir", tmpdir])
        return result

class TestSimpleDependencyBadProfile(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                '+any_config': "{{ target.name }}",
                '+enabled': "{{ target.name in ['redshift', 'postgres'] | as_bool }}"
            }
        }

    def postgres_profile(self):
        # Need to set the environment variable here initially because
        # the unittest setup does a load_config.
        os.environ['PROFILE_TEST_HOST'] = self.database_host
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': "{{ env_var('PROFILE_TEST_HOST') }}",
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                },
                'target': 'default2'
            }
        }

    @use_profile('postgres')
    def test_postgres_deps_bad_profile(self):
        del os.environ['PROFILE_TEST_HOST']
        self.run_dbt(["deps"])

    @use_profile('postgres')
    def test_postgres_clean_bad_profile(self):
        del os.environ['PROFILE_TEST_HOST']
        self.run_dbt(["clean"])
