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



class MetricsTest(BasePPTest):

    @use_profile('postgres')
    def test_postgres_env_vars_models(self):
        self.setup_directories()
        # initial run
        self.copy_file('test-files/people.sql', 'models/people.sql')
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 1)

        # Add metrics yaml file
        self.copy_file('test-files/people_metrics.yml', 'models/people_metrics.yml')
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        self.assertEqual(len(manifest.metrics), 2)
        metric_people_id = 'metric.test.number_of_people'
        metric_tenure_id = 'metric.test.collective_tenure'
        metric_people = manifest.metrics[metric_people_id]
        metric_tenure = manifest.metrics[metric_tenure_id]
        expected_meta = {'my_meta': 'testing'}
        self.assertEqual(metric_people.meta, expected_meta)
        self.assertEqual(metric_people.refs, [['people']])
        self.assertEqual(metric_tenure.refs, [['people']])
        expected_depends_on_nodes = ['model.test.people']
        self.assertEqual(metric_people.depends_on.nodes, expected_depends_on_nodes)

        # Change metrics yaml files
        self.copy_file('test-files/people_metrics2.yml', 'models/people_metrics.yml')
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        metric_people = manifest.metrics[metric_people_id]
        expected_meta = {'my_meta': 'replaced'}
        self.assertEqual(metric_people.meta, expected_meta)
        expected_depends_on_nodes = ['model.test.people']
        self.assertEqual(metric_people.depends_on.nodes, expected_depends_on_nodes)

