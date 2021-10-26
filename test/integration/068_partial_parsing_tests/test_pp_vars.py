from dbt.exceptions import CompilationException, UndefinedMacroException
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
            'data-paths': ['seeds'],
            'test-paths': ['tests'],
            'macro-paths': ['macros'],
            'analysis-paths': ['analyses'],
            'snapshot-paths': ['snapshots'],
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
        os.mkdir(os.path.join(self.test_root_dir, 'seeds'))
        os.mkdir(os.path.join(self.test_root_dir, 'macros'))
        os.mkdir(os.path.join(self.test_root_dir, 'analyses'))
        os.mkdir(os.path.join(self.test_root_dir, 'snapshots'))



class EnvVarTest(BasePPTest):

    @use_profile('postgres')
    def test_postgres_env_vars_models(self):
        self.setup_directories()
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        # initial run
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # copy a file with an env_var call without an env_var
        self.copy_file('test-files/env_var_model.sql', 'models/env_var_model.sql')
        with self.assertRaises(UndefinedMacroException):
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

        # delete the env var to cleanup
        del os.environ['ENV_VAR_TEST']
