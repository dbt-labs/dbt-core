from dbt.exceptions import CompilationException, ParsingException
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import ParseFileType
from dbt.contracts.results import TestStatus
from dbt.parser.partial import special_override_macros
from test.integration.base import DBTIntegrationTest, use_profile, normalize, get_manifest
import shutil
import os


# Note: every test case needs to have separate directories, otherwise
# they will interfere with each other when tests are multi-threaded

class BasePPTest(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_068A"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'test-paths': ['tests'],
            'macro-paths': ['macros'],
            'seeds': {
                'quote_columns': False,
            },
        }

    def setup_directories(self):
        # Create the directories for the test in the `self.test_root_dir`
        # directory after everything else is symlinked. We can copy to and
        # delete files in this directory without tests interfering with each other.
        os.mkdir(os.path.join(self.test_root_dir, 'models'))
        os.mkdir(os.path.join(self.test_root_dir, 'tests'))
        os.mkdir(os.path.join(self.test_root_dir, 'macros'))
        os.mkdir(os.path.join(self.test_root_dir, 'seeds'))



class EnvVarTest(BasePPTest):

    @use_profile('postgres')
    def test_postgres_env_vars_models(self):
        self.setup_directories()
        self.copy_file('test-files/model_color.sql', 'models/model_color.sql')
        # initial run
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # copy a file with an env_var call without an env_var
        self.copy_file('test-files/env_var_model.sql', 'models/env_var_model.sql')
        with self.assertRaises(ParsingException):
            results = self.run_dbt(["--partial-parse", "run"])

        # set the env var
        os.environ['ENV_VAR_TEST'] = 'TestingEnvVars'
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        expected_env_vars = {"ENV_VAR_TEST": "TestingEnvVars"}
        self.assertEqual(expected_env_vars, manifest.env_vars)
        model_id = 'model.test.env_var_model'
        model = manifest.nodes[model_id]
        model_created_at = model.created_at

        # change the env var
        os.environ['ENV_VAR_TEST'] = 'second'
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        expected_env_vars = {"ENV_VAR_TEST": "second"}
        self.assertEqual(expected_env_vars, manifest.env_vars)
        self.assertNotEqual(model_created_at, manifest.nodes[model_id].created_at)

        # set an env_var in a schema file
        self.copy_file('test-files/env_var_schema.yml', 'models/schema.yml')
        self.copy_file('test-files/env_var_model_one.sql', 'models/model_one.sql')
        with self.assertRaises(ParsingException):
            results = self.run_dbt(["--partial-parse", "run"])

        # actually set the env_var
        os.environ['TEST_SCHEMA_VAR'] = 'view'
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        expected_env_vars = {"ENV_VAR_TEST": "second", "TEST_SCHEMA_VAR": "view"}
        self.assertEqual(expected_env_vars, manifest.env_vars)

        # env vars in a source
        os.environ['ENV_VAR_DATABASE'] = 'dbt'
        os.environ['ENV_VAR_SEVERITY'] = 'warn'
        self.copy_file('test-files/raw_customers.csv', 'seeds/raw_customers.csv')
        self.copy_file('test-files/env_var-sources.yml', 'models/sources.yml')
        self.run_dbt(['--partial-parse', 'seed'])
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        expected_env_vars = {"ENV_VAR_TEST": "second", "TEST_SCHEMA_VAR": "view", "ENV_VAR_DATABASE": "dbt", "ENV_VAR_SEVERITY": "warn"}
        self.assertEqual(expected_env_vars, manifest.env_vars)
        self.assertEqual(len(manifest.sources), 1)
        source_id = 'source.test.seed_sources.raw_customers'
        source = manifest.sources[source_id]
        self.assertEqual(source.database, 'dbt')
        schema_file = manifest.files[source.file_id]
        test_id = 'test.test.source_not_null_seed_sources_raw_customers_id.e39ee7bf0d'
        test_node = manifest.nodes[test_id]
        self.assertEqual(test_node.config.severity, 'WARN')

        # Change severity env var
        os.environ['ENV_VAR_SEVERITY'] = 'error'
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        expected_env_vars = {"ENV_VAR_TEST": "second", "TEST_SCHEMA_VAR": "view", "ENV_VAR_DATABASE": "dbt", "ENV_VAR_SEVERITY": "error"}
        self.assertEqual(expected_env_vars, manifest.env_vars)
        source_id = 'source.test.seed_sources.raw_customers'
        source = manifest.sources[source_id]
        schema_file = manifest.files[source.file_id]
        expected_schema_file_env_vars = {'sources': {'seed_sources': ['ENV_VAR_DATABASE', 'ENV_VAR_SEVERITY']}}
        self.assertEqual(expected_schema_file_env_vars, schema_file.env_vars)
        test_node = manifest.nodes[test_id]
        self.assertEqual(test_node.config.severity, 'ERROR')

        # Change database env var
        os.environ['ENV_VAR_DATABASE'] = 'test_dbt'
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        expected_env_vars = {"ENV_VAR_TEST": "second", "TEST_SCHEMA_VAR": "view", "ENV_VAR_DATABASE": "test_dbt", "ENV_VAR_SEVERITY": "error"}
        self.assertEqual(expected_env_vars, manifest.env_vars)
        source = manifest.sources[source_id]
        self.assertEqual(source.database, 'test_dbt')

        # Delete database env var
        del os.environ['ENV_VAR_DATABASE']
        with self.assertRaises(ParsingException):
            results = self.run_dbt(["--partial-parse", "run"])
        os.environ['ENV_VAR_DATABASE'] = 'test_dbt'

        # Add generic test with test kwarg that's rendered late (no curly brackets)
        os.environ['ENV_VAR_DATABASE'] = 'dbt'
        self.copy_file('test-files/test_color.sql', 'macros/test_color.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        # Add source test using test_color and an env_var for color
        self.copy_file('test-files/env_var_schema2.yml', 'models/schema.yml')
        with self.assertRaises(ParsingException):
            results = self.run_dbt(["--partial-parse", "run"])
        os.environ['ENV_VAR_COLOR'] = 'green'
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        test_color_id = 'test.test.check_color_model_one_env_var_ENV_VAR_COLOR___fun.89638de387'
        test_node = manifest.nodes[test_color_id]
        # kwarg was rendered but not changed (it will be rendered again when compiled)
        self.assertEqual(test_node.test_metadata.kwargs['color'], "env_var('ENV_VAR_COLOR')")
        results = self.run_dbt(["--partial-parse", "test"])

        # Add an exposure with an env_var
        os.environ['ENV_VAR_OWNER'] = "John Doe"
        self.copy_file('test-files/env_var_schema3.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        expected_env_vars = {
            "ENV_VAR_TEST": "second",
            "TEST_SCHEMA_VAR": "view",
            "ENV_VAR_DATABASE": "dbt",
            "ENV_VAR_SEVERITY": "error",
            "ENV_VAR_COLOR": 'green',
            "ENV_VAR_OWNER": "John Doe",
        }
        self.assertEqual(expected_env_vars, manifest.env_vars)
        exposure = list(manifest.exposures.values())[0]
        schema_file = manifest.files[exposure.file_id]
        expected_sf_env_vars = {
            'models': {
                'model_one': ['TEST_SCHEMA_VAR', 'ENV_VAR_COLOR']
            },
            'exposures': {
                'proxy_for_dashboard': ['ENV_VAR_OWNER']
            }
        }
        self.assertEqual(expected_sf_env_vars, schema_file.env_vars)

        # add a macro and a macro schema file
        os.environ['ENV_VAR_SOME_KEY'] = 'toodles'
        self.copy_file('test-files/env_var_macro.sql', 'macros/env_var_macro.sql')
        self.copy_file('test-files/env_var_macros.yml', 'macros/env_var_macros.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        expected_env_vars = {
            "ENV_VAR_TEST": "second",
            "TEST_SCHEMA_VAR": "view",
            "ENV_VAR_DATABASE": "dbt",
            "ENV_VAR_SEVERITY": "error",
            "ENV_VAR_COLOR": 'green',
            "ENV_VAR_OWNER": "John Doe",
            "ENV_VAR_SOME_KEY": "toodles",
        }
        self.assertEqual(expected_env_vars, manifest.env_vars)
        macro_id = 'macro.test.do_something'
        macro = manifest.macros[macro_id]
        self.assertEqual(macro.meta, {"some_key": "toodles"})
        # change the env var
        os.environ['ENV_VAR_SOME_KEY'] = 'dumdedum'
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        macro = manifest.macros[macro_id]
        self.assertEqual(macro.meta, {"some_key": "dumdedum"})

        # Add a schema file with a test on model_color and env_var in test enabled config
        self.copy_file('test-files/env_var_model_test.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        model_color = manifest.nodes['model.test.model_color']
        schema_file = manifest.files[model_color.patch_path]
        expected_env_vars = {'models': {'model_one': ['TEST_SCHEMA_VAR', 'ENV_VAR_COLOR'], 'model_color': ['ENV_VAR_ENABLED']}, 'exposures': {'proxy_for_dashboard': ['ENV_VAR_OWNER']}}
        self.assertEqual(expected_env_vars, schema_file.env_vars)

        # Add a metrics file with env_vars
        os.environ['ENV_VAR_METRICS'] = 'TeStInG'
        self.copy_file('test-files/people.sql', 'models/people.sql')
        self.copy_file('test-files/env_var_metrics.yml', 'models/metrics.yml')
        results = self.run_dbt(["run"])
        manifest = get_manifest()
        self.assertIn('ENV_VAR_METRICS', manifest.env_vars)
        self.assertEqual(manifest.env_vars['ENV_VAR_METRICS'], 'TeStInG')
        metric_node = manifest.metrics['metric.test.number_of_people']
        self.assertEqual(metric_node.meta, {'my_meta': 'TeStInG'})

        # Change metrics env var
        os.environ['ENV_VAR_METRICS'] = 'Changed!'
        results = self.run_dbt(["run"])
        manifest = get_manifest()
        metric_node = manifest.metrics['metric.test.number_of_people']
        self.assertEqual(metric_node.meta, {'my_meta': 'Changed!'})

        # delete the env vars to cleanup
        del os.environ['ENV_VAR_TEST']
        del os.environ['ENV_VAR_SEVERITY']
        del os.environ['ENV_VAR_DATABASE']
        del os.environ['TEST_SCHEMA_VAR']
        del os.environ['ENV_VAR_COLOR']
        del os.environ['ENV_VAR_SOME_KEY']
        del os.environ['ENV_VAR_OWNER']
        del os.environ['ENV_VAR_METRICS']


class ProjectEnvVarTest(BasePPTest):

    @property
    def project_config(self):
        # Need to set the environment variable here initially because
        # the unittest setup does a load_config.
        os.environ['ENV_VAR_NAME'] = "Jane Smith"
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'test-paths': ['tests'],
            'macro-paths': ['macros'],
            'seeds': {
                'quote_columns': False,
            },
            'models': {
                '+meta': {
                    'meta_name': "{{ env_var('ENV_VAR_NAME') }}"
                }
            }
        }

    @use_profile('postgres')
    def test_postgres_project_env_vars(self):

        # Initial run
        self.setup_directories()
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        state_check = manifest.state_check
        model_id = 'model.test.model_one'
        model = manifest.nodes[model_id]
        self.assertEqual(model.config.meta['meta_name'], 'Jane Smith')
        env_vars_hash_checksum = state_check.project_env_vars_hash.checksum

        # Change the environment variable
        os.environ['ENV_VAR_NAME'] = "Jane Doe"
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        model = manifest.nodes[model_id]
        self.assertEqual(model.config.meta['meta_name'], 'Jane Doe')
        self.assertNotEqual(env_vars_hash_checksum, manifest.state_check.project_env_vars_hash.checksum)

        # cleanup
        del os.environ['ENV_VAR_NAME']

class ProfileEnvVarTest(BasePPTest):

    @property
    def profile_config(self):
        # Need to set these here because the base integration test class
        # calls 'load_config' before the tests are run.
        # Note: only the specified profile is rendered, so there's no
        # point it setting env_vars in non-used profiles.
        os.environ['ENV_VAR_USER'] = 'root'
        os.environ['ENV_VAR_PASS'] = 'password'
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'dev': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': "root",
                        'pass': "password",
                        'user': "{{ env_var('ENV_VAR_USER') }}",
                        'pass': "{{ env_var('ENV_VAR_PASS') }}",
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                },
                'target': 'dev'
            }
        }

    @use_profile('postgres')
    def test_postgres_profile_env_vars(self):

        # Initial run
        os.environ['ENV_VAR_USER'] = 'root'
        os.environ['ENV_VAR_PASS'] = 'password'
        self.setup_directories()
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        results = self.run_dbt(["run"])
        manifest = get_manifest()
        env_vars_checksum = manifest.state_check.profile_env_vars_hash.checksum

        # Change env_vars, the user doesn't exist, this should fail
        os.environ['ENV_VAR_USER'] = 'fake_user'
        (results, log_output) = self.run_dbt_and_capture(["run"], expect_pass=False)
        self.assertTrue('env vars used in profiles.yml have changed' in log_output)
        manifest = get_manifest()
        self.assertNotEqual(env_vars_checksum, manifest.state_check.profile_env_vars_hash.checksum)

